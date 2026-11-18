//! Read-only rendered-frame observation for developer tooling and automation.
//!
//! GPUI's accessibility tree is the canonical description of a rendered UI.
//! This module exposes completed copies of that tree without activating an OS
//! accessibility adapter, and provides a final overlay pass for visual tools.
//! Each observed draw also reports what it cost and which views rendered or
//! replayed their previous output from cache.

use crate::{App, Bounds, EntityId, GlobalElementId, Pixels, Window};
use accesskit::{Node, NodeId, Role, TreeUpdate};
use collections::{FxHashMap, FxHashSet};
use std::{
    collections::BTreeMap,
    fmt::Write as _,
    sync::{Arc, OnceLock},
    time::Duration,
};

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
    /// Position of each node in `tree.nodes`, built on the first lookup rather
    /// than during the draw, so an observer that never looks pays nothing.
    positions: OnceLock<FxHashMap<NodeId, usize>>,
}

impl AccessibilityFrame {
    pub(crate) fn new(tree: TreeUpdate, nodes: FxHashMap<NodeId, FrameNode>) -> Self {
        Self {
            tree,
            nodes,
            positions: OnceLock::new(),
        }
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
        let positions = self.positions.get_or_init(|| {
            let mut positions = FxHashMap::default();
            for (position, (id, _)) in self.tree.nodes.iter().enumerate() {
                positions.entry(*id).or_insert(position);
            }
            positions
        });
        positions
            .get(&node.accessibility_id)
            .map(|position| &self.tree.nodes[*position].1)
    }
}

/// Why a view called `render` in a frame instead of replaying its previous
/// output.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ViewRenderCause {
    /// The view is not embedded with `cached`, so it renders whenever the
    /// element tree containing it is built.
    Uncached,
    /// Inspector picking disables view caching.
    CachingDisabled,
    /// [`Window::refresh`] was pending, which renders every cached view.
    Refresh,
    /// The view had no output from the previous frame to replay.
    FirstDraw,
    /// The view, or a view drawn inside it, was notified.
    Notified,
    /// A containing cached view rendered. GPUI renders every cached view
    /// inside a cached view that renders.
    AncestorRendered,
    /// The view's bounds, content mask, or text style changed.
    LayoutChanged,
    /// The containing element's observation redaction boundary changed.
    ObservationChanged,
}

/// How a view produced its output for one frame.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ViewDrawOutcome {
    /// The previous frame's prepaint and paint were replayed without calling
    /// `render`.
    Reused,
    /// The view called `render`.
    Rendered(ViewRenderCause),
}

/// One view drawn in an observed frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ViewDraw {
    entity_id: EntityId,
    type_name: &'static str,
    outcome: ViewDrawOutcome,
}

impl ViewDraw {
    /// Return the entity that backs the view.
    pub fn entity_id(&self) -> EntityId {
        self.entity_id
    }

    /// Return the view's `Render` type name, or an empty string when the view
    /// has not rendered since observation began.
    pub fn type_name(&self) -> &'static str {
        self.type_name
    }

    /// Return whether the view rendered or replayed its previous output.
    pub fn outcome(&self) -> ViewDrawOutcome {
        self.outcome
    }
}

/// Cost and view-cache activity of one completed observed draw.
#[derive(Debug)]
pub struct DrawnFrame<'a> {
    draw: Duration,
    observation: Duration,
    views: &'a [ViewDraw],
}

impl DrawnFrame<'_> {
    /// Return the wall time of the whole `Window::draw`: rendering, layout,
    /// prepaint, and paint. This is the interval GPUI's profiler records as
    /// `FrameTiming::draw_duration` when that feature is enabled.
    pub fn draw_duration(&self) -> Duration {
        self.draw
    }

    /// Return the part of [`Self::draw_duration`] spent only because the frame
    /// was observed: finishing the accessibility tree when no platform adapter
    /// wanted it, building the [`AccessibilityFrame`], and running observer
    /// callbacks. Recording each element's accessibility node during prepaint
    /// is interleaved with the application's own work and is not included.
    pub fn observation_duration(&self) -> Duration {
        self.observation
    }

    /// Return every view drawn in the frame, in draw order, with whether it
    /// rendered or replayed its previous output.
    pub fn views(&self) -> &[ViewDraw] {
        self.views
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

    /// Called after prepaint has completed with a shared handle to the
    /// canonical accessibility tree.
    ///
    /// An observer that keeps the frame, for example to convert it later off
    /// the UI thread, overrides this instead of [`Self::accessibility_updated`]
    /// and retains the handle rather than copying the tree during the draw. The
    /// default forwards to [`Self::accessibility_updated`].
    fn accessibility_frame(&self, frame: &Arc<AccessibilityFrame>) {
        self.accessibility_updated(frame);
    }

    /// Called immediately before the normal paint pass.
    fn paint_started(&self) {}

    /// Paint an overlay after all normal elements have painted.
    fn paint_overlay(&self, _window: &mut Window, _cx: &mut App) {}

    /// Called after normal paint and observer overlays have completed.
    fn frame_finished(&self) {}

    /// Called when `Window::draw` returns with what the draw cost and which
    /// views rendered or replayed from cache.
    fn frame_drawn(&self, _frame: &DrawnFrame<'_>) {}
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
    /// Whether [`Window::refresh`] was pending when this frame began.
    window_refresh: bool,
    views: Vec<ViewDraw>,
    /// `Render` type name of every view drawn in this frame. A view that
    /// replays its previous output does not render, so its name is carried
    /// forward from the frame it was last rendered in.
    view_types: FxHashMap<EntityId, &'static str>,
    observation: Duration,
}

#[derive(Clone, Debug)]
pub(crate) struct FrameParent {
    path: String,
    /// Position of the parent's node in the builder that entered it. A
    /// rolled-back transaction can truncate that node, so every use checks the
    /// path before trusting it.
    index: usize,
}

/// The builder's state at one point in prepaint.
///
/// Only a node that is open can collect text, so the open nodes are the only
/// ones that existed at the checkpoint and can change after it. Recording just
/// those keeps a checkpoint proportional to the element depth rather than to
/// every node drawn so far, which matters because each cached view takes two.
#[derive(Clone, Debug, Default)]
pub(crate) struct FrameCheckpoint {
    node_count: usize,
    open: Vec<(usize, String, String)>,
}

impl FrameBuilder {
    pub(crate) fn begin(&mut self, enabled: bool) {
        self.nodes.clear();
        self.parents.clear();
        self.enabled = enabled;
        self.window_refresh = false;
        self.views.clear();
        self.view_types.clear();
        self.observation = Duration::ZERO;
    }

    pub(crate) fn enable(&mut self) {
        self.enabled = true;
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn set_window_refresh(&mut self, window_refresh: bool) {
        self.window_refresh = window_refresh;
    }

    pub(crate) fn window_refresh(&self) -> bool {
        self.window_refresh
    }

    /// Record the `Render` type of a view that is rendering.
    pub(crate) fn note_view_type(&mut self, entity_id: EntityId, type_name: &'static str) {
        if self.enabled {
            self.view_types.insert(entity_id, type_name);
        }
    }

    /// Record how a view produced its output. `previous` is the last rendered
    /// frame, which names a view that replays without rendering.
    pub(crate) fn view_drawn(
        &mut self,
        entity_id: EntityId,
        outcome: ViewDrawOutcome,
        previous: &FrameBuilder,
    ) {
        if !self.enabled {
            return;
        }
        if outcome == ViewDrawOutcome::Reused
            && let Some(type_name) = previous.view_types.get(&entity_id)
        {
            self.view_types.entry(entity_id).or_insert(type_name);
        }
        self.views.push(ViewDraw {
            entity_id,
            type_name: "",
            outcome,
        });
    }

    pub(crate) fn add_observation(&mut self, duration: Duration) {
        self.observation += duration;
    }

    /// Name every view drawn in the finished frame and describe its cost.
    pub(crate) fn drawn(&mut self, draw: Duration) -> DrawnFrame<'_> {
        for view in &mut self.views {
            if let Some(type_name) = self.view_types.get(&view.entity_id) {
                view.type_name = type_name;
            }
        }
        DrawnFrame {
            draw,
            observation: self.observation,
            views: &self.views,
        }
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
            index: self.nodes.len(),
        };
        self.nodes.push(node);
        self.parents.push(parent);
        true
    }

    /// Keep interaction provenance when an external wrapper forwards a Div's
    /// prepaint but does not know about Element::frame_node. Its accessibility
    /// hooks registered the node; the inner Interactivity supplies these facts
    /// before children contribute text. Only the same open node may be refined.
    pub(crate) fn refine_interactivity(
        &mut self,
        global_id: Option<&GlobalElementId>,
        data: FrameNodeData,
    ) {
        if !self.enabled {
            return;
        }
        let Some(id) = global_id.map(GlobalElementId::accesskit_node_id) else {
            return;
        };
        let Some(node) = self
            .parents
            .last()
            .and_then(|parent| self.nodes.get_mut(parent.index))
        else {
            return;
        };
        if node.accessibility_id != id {
            return;
        }
        node.redacted |= data.redacted;
        for action in data.actions {
            if !node.actions.contains(&action) {
                node.actions.push(action);
            }
        }
        for (key, value) in data.metadata {
            node.metadata.entry(key).or_insert(value);
        }
    }

    /// Return the index of `parent`'s node in this builder, when it is still
    /// there.
    fn parent_index(nodes: &[FrameNode], parent: &FrameParent) -> Option<usize> {
        if nodes
            .get(parent.index)
            .is_some_and(|node| node.path == parent.path)
        {
            return Some(parent.index);
        }
        nodes.iter().rposition(|node| node.path == parent.path)
    }

    pub(crate) fn exit(&mut self, entered: bool) {
        if entered {
            self.parents.pop();
        }
    }

    /// Text inside a redacted element must not contribute to any ancestor.
    pub(crate) fn text_is_redacted(&self) -> bool {
        self.parents.iter().any(|parent| {
            Self::parent_index(&self.nodes, parent)
                .is_some_and(|index| self.nodes[index].redacted)
        })
    }

    pub(crate) fn add_text(&mut self, text: &str) {
        if !self.enabled || self.text_is_redacted() {
            return;
        }
        let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
        if normalized.is_empty() {
            return;
        }
        for parent in &self.parents {
            if let Some(index) = Self::parent_index(&self.nodes, parent) {
                let node = &mut self.nodes[index];
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
        if !self.enabled {
            return FrameCheckpoint {
                node_count: self.nodes.len(),
                open: Vec::new(),
            };
        }
        FrameCheckpoint {
            node_count: self.nodes.len(),
            open: self
                .parents
                .iter()
                .filter_map(|parent| {
                    let index = Self::parent_index(&self.nodes, parent)?;
                    let node = &self.nodes[index];
                    Some((index, node.path.clone(), node.content_text.clone()))
                })
                .collect(),
        }
    }

    pub(crate) fn restore(&mut self, checkpoint: &FrameCheckpoint) {
        self.nodes.truncate(checkpoint.node_count);
        for (index, path, content) in &checkpoint.open {
            if let Some(node) = self.nodes.get_mut(*index)
                && node.path == *path
            {
                node.content_text.clone_from(content);
            }
        }
    }

    pub(crate) fn reuse(
        &mut self,
        rendered: &FrameBuilder,
        start: &FrameCheckpoint,
        end: &FrameCheckpoint,
    ) {
        // The replayed range added text to the nodes open around it. Those
        // nodes are open around the replay too, found by the same path.
        let text_is_redacted = self.text_is_redacted();
        for (_, path, before) in &start.open {
            if !text_is_redacted
                && let Some((_, _, after)) = end.open.iter().find(|(_, candidate, _)| candidate == path)
                && let Some(delta) = after.strip_prefix(before.as_str())
                && !delta.is_empty()
                && let Some(index) = self
                    .parents
                    .iter()
                    .rev()
                    .find(|parent| parent.path == *path)
                    .and_then(|parent| Self::parent_index(&self.nodes, parent))
                    .or_else(|| self.nodes.iter().position(|node| node.path == *path))
            {
                let node = &mut self.nodes[index];
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
