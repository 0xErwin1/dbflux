//! Pure tree-walking helpers for the WHERE filter and the join ON condition
//! trees. Kept separate from `panel.rs` so the panel module stays focused on
//! GPUI entity lifecycle and state mutation rather than recursive traversal.
//!
//! All functions here are free of `&self` and have no GPUI dependencies; the
//! panel module composes them with its own state.

use std::collections::HashSet;

use dbflux_core::{FilterNode, JoinFilterNode, JoinPredicate};

// ---------------------------------------------------------------------------
// Filter tree (WHERE)
// ---------------------------------------------------------------------------

/// Inserts `node` as the last child of the group reached by walking `path`
/// from `root`. No-op if `path` does not resolve to a `Group`.
#[expect(
    clippy::indexing_slicing,
    reason = "path[0] is reached only on the !path.is_empty() branch, which also makes [1..] a valid range"
)]
pub(crate) fn insert_filter_at_path(root: &mut FilterNode, path: &[usize], node: FilterNode) {
    if path.is_empty() {
        if let FilterNode::Group { children, .. } = root {
            children.push(node);
        }
    } else if let FilterNode::Group { children, .. } = root
        && let Some(child) = children.get_mut(path[0])
    {
        insert_filter_at_path(child, &path[1..], node);
    }
}

/// Removes the child at the position addressed by `path` from its parent
/// group. No-op when `path` is empty or out of range.
//
// The doc comment's empty-path claim holds only because both panel callers
// (remove_filter_node and remove_filter_node_for in panel/filters.rs) handle
// empty paths themselves before calling; this function does not re-check, so
// a direct call with an empty path would panic at path[0]. In the
// len() == 1 branch the single element exists; the else branch then runs on
// multi-element paths from those callers.
#[expect(
    clippy::indexing_slicing,
    reason = "path[0] exists in the len() == 1 branch; the else branch needs a non-empty path, which the external callers (remove_filter_node and remove_filter_node_for) ensure by handling path.is_empty() first and this function's own recursive call guarantees by passing a slice of a path of length two or more; this function does not check it itself"
)]
pub(crate) fn remove_filter_at_path(root: &mut FilterNode, path: &[usize]) {
    if path.len() == 1 {
        if let FilterNode::Group { children, .. } = root
            && path[0] < children.len()
        {
            children.remove(path[0]);
        }
    } else if let FilterNode::Group { children, .. } = root
        && let Some(child) = children.get_mut(path[0])
    {
        remove_filter_at_path(child, &path[1..]);
    }
}

/// Returns a mutable reference to the node addressed by `path`, or `None` if
/// the path is invalid (descends through a non-group or off the end).
#[expect(
    clippy::indexing_slicing,
    reason = "the empty path returns early, so path[0] exists and [1..] is a valid range"
)]
pub(crate) fn filter_node_at_path_mut<'a>(
    root: &'a mut FilterNode,
    path: &[usize],
) -> Option<&'a mut FilterNode> {
    if path.is_empty() {
        return Some(root);
    }
    if let FilterNode::Group { children, .. } = root
        && let Some(child) = children.get_mut(path[0])
    {
        return filter_node_at_path_mut(child, &path[1..]);
    }
    None
}

/// Collects the `node_id` of every `Predicate` in the tree into `ids`.
pub(crate) fn collect_filter_predicate_ids(node: &FilterNode, ids: &mut HashSet<u64>) {
    match node {
        FilterNode::Predicate(pred) => {
            ids.insert(pred.node_id);
        }
        FilterNode::Group { children, .. } => {
            for child in children {
                collect_filter_predicate_ids(child, ids);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Join condition tree (ON)
// ---------------------------------------------------------------------------

/// Mutable lookup by path inside a join ON tree.
pub(crate) fn join_node_at_path_mut<'a>(
    root: &'a mut JoinFilterNode,
    path: &[usize],
) -> Option<&'a mut JoinFilterNode> {
    let mut cur = root;
    for &ix in path {
        match cur {
            JoinFilterNode::Group { children, .. } => {
                cur = children.get_mut(ix)?;
            }
            _ => return None,
        }
    }
    Some(cur)
}

/// Walks the tree looking for the `Predicate` whose `node_id` equals `target`
/// and applies `f` to it. Returns `true` once a match is found.
///
/// `f` is taken as `&mut dyn FnMut` rather than `impl FnMut`: recursive calls
/// with an `impl` bound monomorphize through ever-deeper `&mut &mut &mut …`
/// reference types and blow the recursion limit. The `dyn` form keeps the
/// trait object stable across recursion at the cost of one virtual call per
/// matched leaf — negligible here.
pub(crate) fn set_join_predicate_field(
    node: &mut JoinFilterNode,
    target: u64,
    f: &mut dyn FnMut(&mut JoinPredicate),
) -> bool {
    match node {
        JoinFilterNode::Predicate(p) if p.node_id == target => {
            f(p);
            true
        }
        JoinFilterNode::Group { children, .. } => {
            for child in children.iter_mut() {
                if set_join_predicate_field(child, target, f) {
                    return true;
                }
            }
            false
        }
        _ => false,
    }
}

/// Collects the `node_id` of every `Predicate` in a join ON tree.
pub(crate) fn collect_join_predicate_ids(node: &JoinFilterNode, acc: &mut HashSet<u64>) {
    match node {
        JoinFilterNode::Predicate(p) => {
            acc.insert(p.node_id);
        }
        JoinFilterNode::Group { children, .. } => {
            for child in children {
                collect_join_predicate_ids(child, acc);
            }
        }
    }
}
