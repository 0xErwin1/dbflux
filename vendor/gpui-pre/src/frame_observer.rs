//! Read-only rendered-frame observation for developer tooling and automation.
//!
//! GPUI's accessibility tree is the canonical description of a rendered UI.
//! This module exposes completed copies of that tree without activating an OS
//! accessibility adapter, and provides a final overlay pass for visual tools.

use crate::{App, Bounds, GlobalElementId, Pixels, Window};
use accesskit::{Node, NodeId, Role, TreeUpdate};
use collections::{FxHashMap, FxHashSet};
use std::{collections::BTreeMap, fmt::Write as _, sync::Arc};

/// Interaction details that are meaningful to visual tooling but are not
/// represented by an AccessKit action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameAction {
    /// Moving the pointer over the element can change or reveal UI.
    Hover,
    /// The element can initiate a drag operation.
    Drag,
    /// The element consumes scrolling or owns scroll state.
    Scroll,
    /// The element accepts complete or selected text replacement.
    SetText,
    /// The element accepts a new string or numeric value.
    SetValue,
}

/// GPUI-specific provenance associated with one AccessKit node.
#[derive(Clone, Debug, PartialEq)]
pub struct FrameNode {
    /// The element ID while the frame is being collected, replaced by the
    /// frame-unique identity when the frame is finished.
    id: String,
    path: String,
    /// Byte offset of each path segment within `path`.
    segment_starts: Vec<usize>,
    /// The parent's identity, resolved when the frame is finished.
    parent: Option<String>,
    /// The complete element path of the nearest observed ancestor.
    parent_path: Option<String>,
    bounds: Bounds<Pixels>,
    actions: Vec<FrameAction>,
    metadata: BTreeMap<String, String>,
    redacted: bool,
    content_text: String,
    accessibility_id: NodeId,
    fallback_role: Role,
}

impl FrameNode {
    /// Return the identity of this node within its frame.
    ///
    /// This is the element's own ID wherever that ID names one element, and a
    /// longer trailing run of the element path wherever it does not.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Return the complete GPUI element path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Return the identity of the nearest rendered ancestor with an element ID.
    pub fn parent(&self) -> Option<&str> {
        self.parent.as_deref()
    }

    /// Return the trailing `depth` segments of the complete element path.
    ///
    /// Element IDs may contain the path separator, so segments are cut at the
    /// recorded offsets rather than by splitting the rendered path.
    fn path_suffix(&self, depth: usize) -> &str {
        let start = self
            .segment_starts
            .len()
            .checked_sub(depth)
            .and_then(|index| self.segment_starts.get(index).copied())
            .unwrap_or(0);
        &self.path[start..]
    }

    /// Return window-relative bounds in logical pixels.
    pub fn bounds(&self) -> Bounds<Pixels> {
        self.bounds
    }

    /// Return interactions not represented by AccessKit.
    pub fn actions(&self) -> &[FrameAction] {
        &self.actions
    }

    /// Return bounded, application-provided context for developer tooling.
    pub fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    /// Return whether text or value content must not be exposed to tooling.
    pub fn is_redacted(&self) -> bool {
        self.redacted
    }

    /// Return normalized descendant text collected during prepaint, or an
    /// empty string when this node is redacted.
    pub fn content_text(&self) -> &str {
        if self.redacted {
            ""
        } else {
            &self.content_text
        }
    }

    /// Return the AccessKit node ID derived from the complete GPUI element path.
    pub fn accessibility_id(&self) -> NodeId {
        self.accessibility_id
    }

    /// Return the role inferred from GPUI behavior when no explicit role exists.
    pub fn fallback_role(&self) -> Role {
        self.fallback_role
    }
}

/// A completed rendered-frame snapshot backed by GPUI's AccessKit tree.
#[derive(Clone, Debug)]
pub struct AccessibilityFrame {
    tree: TreeUpdate,
    nodes: FxHashMap<NodeId, FrameNode>,
}

impl AccessibilityFrame {
    pub(crate) fn new(tree: TreeUpdate, nodes: FxHashMap<NodeId, FrameNode>) -> Self {
        Self { tree, nodes }
    }

    /// Return the canonical AccessKit update for this frame.
    pub fn tree(&self) -> &TreeUpdate {
        &self.tree
    }

    /// Return GPUI provenance for an AccessKit node.
    pub fn node(&self, id: NodeId) -> Option<&FrameNode> {
        self.nodes.get(&id)
    }

    /// Iterate over GPUI provenance keyed by AccessKit node ID.
    pub fn nodes(&self) -> impl Iterator<Item = (NodeId, &FrameNode)> {
        self.nodes.iter().map(|(id, node)| (*id, node))
    }

    /// Return the canonical AccessKit node for a rendered GPUI node, when one
    /// was explicitly included in the accessibility tree.
    pub fn accessibility_node(&self, node: &FrameNode) -> Option<&Node> {
        self.tree
            .nodes
            .iter()
            .find_map(|(id, value)| (*id == node.accessibility_id).then_some(value))
    }
}

/// Receives completed rendered frames and may paint a final overlay.
///
/// A window retains observers weakly. The owner must retain the corresponding
/// [`Arc`] for as long as observation should continue.
pub trait FrameObserver: Send + Sync + 'static {
    /// Called immediately before GPUI begins constructing a frame.
    fn frame_started(&self, _window: &Window) {}

    /// Called after prepaint has completed with the canonical accessibility tree.
    fn accessibility_updated(&self, _frame: &AccessibilityFrame) {}

    /// Called immediately before the normal paint pass.
    fn paint_started(&self) {}

    /// Paint an overlay after all normal elements have painted.
    fn paint_overlay(&self, _window: &mut Window, _cx: &mut App) {}

    /// Called after normal paint and observer overlays have completed.
    fn frame_finished(&self) {}
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq)]
pub struct FrameNodeData {
    #[doc(hidden)]
    pub actions: Vec<FrameAction>,
    #[doc(hidden)]
    pub metadata: BTreeMap<String, String>,
    #[doc(hidden)]
    pub redacted: bool,
    #[doc(hidden)]
    pub fallback_role: Role,
}

impl Default for FrameNodeData {
    fn default() -> Self {
        Self {
            actions: Vec::new(),
            metadata: BTreeMap::new(),
            redacted: false,
            fallback_role: Role::Group,
        }
    }
}

pub(crate) fn live_observers(
    observers: &mut Vec<std::sync::Weak<dyn FrameObserver>>,
) -> Vec<Arc<dyn FrameObserver>> {
    let live = observers
        .iter()
        .filter_map(std::sync::Weak::upgrade)
        .collect();
    observers.retain(|observer| observer.strong_count() > 0);
    live
}

/// In-progress rendered element graph. The public snapshot joins this graph to
/// AccessKit after prepaint, so accessibility semantics remain authoritative.
#[derive(Clone, Debug, Default)]
pub(crate) struct FrameBuilder {
    nodes: Vec<FrameNode>,
    parents: Vec<FrameParent>,
    enabled: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct FrameParent {
    path: String,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct FrameCheckpoint {
    node_count: usize,
    content: Vec<(String, String)>,
}

impl FrameBuilder {
    pub(crate) fn begin(&mut self, enabled: bool) {
        self.nodes.clear();
        self.parents.clear();
        self.enabled = enabled;
    }

    pub(crate) fn enable(&mut self) {
        self.enabled = true;
    }

    pub(crate) fn enter(
        &mut self,
        global_id: Option<&GlobalElementId>,
        data: Option<FrameNodeData>,
        bounds: Bounds<Pixels>,
    ) -> bool {
        let (global_id, data) = match (global_id, data) {
            (Some(global_id), Some(data)) if self.enabled => (global_id, data),
            _ => return false,
        };
        let mut path = String::new();
        let mut segment_starts = Vec::with_capacity(global_id.len());
        for segment in global_id.iter() {
            if !path.is_empty() {
                path.push('.');
            }
            segment_starts.push(path.len());
            let _ = write!(path, "{segment}");
        }
        let id = path[segment_starts.last().copied().unwrap_or(0)..].to_owned();
        let node = FrameNode {
            id,
            parent: None,
            parent_path: self.parents.last().map(|parent| parent.path.clone()),
            segment_starts,
            bounds,
            actions: data.actions,
            metadata: data.metadata,
            redacted: data.redacted,
            content_text: String::new(),
            accessibility_id: global_id.accesskit_node_id(),
            fallback_role: data.fallback_role,
            path,
        };
        let parent = FrameParent {
            path: node.path.clone(),
        };
        self.nodes.push(node);
        self.parents.push(parent);
        true
    }

    pub(crate) fn exit(&mut self, entered: bool) {
        if entered {
            self.parents.pop();
        }
    }

    pub(crate) fn add_text(&mut self, text: &str) {
        if !self.enabled {
            return;
        }
        let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
        if normalized.is_empty() {
            return;
        }
        for parent in &self.parents {
            if let Some(node) = self
                .nodes
                .iter_mut()
                .rev()
                .find(|node| node.path == parent.path)
            {
                if node.redacted {
                    continue;
                }
                if !node.content_text.is_empty() {
                    node.content_text.push(' ');
                }
                node.content_text.push_str(&normalized);
            }
        }
    }

    pub(crate) fn current_parent(&self) -> Option<FrameParent> {
        self.parents.last().cloned()
    }

    pub(crate) fn push_parent(&mut self, parent: Option<FrameParent>) -> bool {
        let Some(parent) = parent else {
            return false;
        };
        self.parents.push(parent);
        true
    }

    pub(crate) fn checkpoint(&self) -> FrameCheckpoint {
        FrameCheckpoint {
            node_count: self.nodes.len(),
            content: self
                .nodes
                .iter()
                .map(|node| (node.path.clone(), node.content_text.clone()))
                .collect(),
        }
    }

    pub(crate) fn restore(&mut self, checkpoint: &FrameCheckpoint) {
        self.nodes.truncate(checkpoint.node_count);
        for (node, (_, content)) in self.nodes.iter_mut().zip(&checkpoint.content) {
            node.content_text.clone_from(content);
        }
    }

    pub(crate) fn reuse(
        &mut self,
        rendered: &FrameBuilder,
        start: &FrameCheckpoint,
        end: &FrameCheckpoint,
    ) {
        for (path, before) in &start.content {
            if let Some((_, after)) = end.content.iter().find(|(candidate, _)| candidate == path)
                && let Some(delta) = after.strip_prefix(before)
                && !delta.is_empty()
                && let Some(node) = self.nodes.iter_mut().find(|node| node.path == *path)
            {
                if !node.content_text.is_empty() {
                    node.content_text.push(' ');
                }
                node.content_text.push_str(delta.trim());
            }
        }
        self.nodes.extend(
            rendered.nodes[start.node_count..end.node_count]
                .iter()
                .cloned(),
        );
    }

    /// Name every observed node so that no two nodes share an identity.
    ///
    /// GPUI guarantees only that the complete element path is unique: the same
    /// element ID is free to repeat across sibling subtrees, which is what a
    /// dock does when it renders several instances of one panel view. A node
    /// therefore keeps its own element ID only while that ID names it alone,
    /// and otherwise takes the shortest trailing run of its element path that
    /// separates it from every other node.
    fn identities(&self) -> Vec<String> {
        let mut identities = vec![String::new(); self.nodes.len()];
        let mut unresolved = (0..self.nodes.len()).collect::<Vec<_>>();
        let mut taken = FxHashSet::<String>::default();
        let mut settled = Vec::new();
        let mut depth = 1;
        while !unresolved.is_empty() {
            let mut counts = FxHashMap::<&str, usize>::default();
            for index in &unresolved {
                *counts
                    .entry(self.nodes[*index].path_suffix(depth))
                    .or_default() += 1;
            }
            // Two nodes can only share a complete path if a caller rendered one
            // element ID twice, which GPUI rejects in a debug build. Number them
            // rather than loop forever.
            let exhausted = unresolved
                .iter()
                .all(|index| self.nodes[*index].path_suffix(depth) == self.nodes[*index].path);
            settled.clear();
            unresolved.retain(|index| {
                let node = &self.nodes[*index];
                let suffix = node.path_suffix(depth);
                // An element ID may contain the separator, so a longer run can
                // spell a shorter one already given to another node.
                if counts.get(suffix).copied() == Some(1) && !taken.contains(suffix) {
                    identities[*index] = suffix.to_owned();
                    settled.push(*index);
                    return false;
                }
                if exhausted {
                    identities[*index] = format!("{}#{index}", node.path);
                    settled.push(*index);
                    return false;
                }
                true
            });
            for index in &settled {
                taken.insert(identities[*index].clone());
            }
            depth += 1;
        }
        identities
    }

    pub(crate) fn finish(&mut self, tree: TreeUpdate) -> AccessibilityFrame {
        self.parents.clear();
        let identities = self.identities();
        let by_path = self
            .nodes
            .iter()
            .zip(&identities)
            .map(|(node, identity)| (node.path.as_str(), identity.as_str()))
            .collect::<FxHashMap<_, _>>();
        let nodes = self
            .nodes
            .iter()
            .zip(&identities)
            .map(|(node, identity)| {
                let mut node = node.clone();
                node.parent = node
                    .parent_path
                    .as_deref()
                    .and_then(|path| by_path.get(path).map(|identity| (*identity).to_owned()));
                node.id.clone_from(identity);
                (node.accessibility_id, node)
            })
            .collect();
        AccessibilityFrame::new(tree, nodes)
    }

    pub(crate) fn accessibility_id(&self, id: &str) -> Option<NodeId> {
        let identities = self.identities();
        let mut matches = self
            .nodes
            .iter()
            .zip(&identities)
            .filter(|(_, identity)| identity.as_str() == id);
        let node_id = matches.next()?.0.accessibility_id;
        matches.next().is_none().then_some(node_id)
    }
}
