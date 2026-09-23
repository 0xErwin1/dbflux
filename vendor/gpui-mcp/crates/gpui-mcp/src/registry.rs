use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use gpui_mcp_protocol::{
    BridgeError, ErrorCode, FrameStats, Highlight, LogEntry, MAX_ID_BYTES, MAX_LABEL_BYTES,
    MAX_METADATA_FIELDS, MAX_METADATA_KEY_BYTES, MAX_METADATA_VALUE_BYTES, MAX_TEXT_BYTES,
    MAX_TREE_NODES, Rect, SemanticDiagnostic, SemanticDiagnosticCode, UiNode, UiTree,
    WindowGeometry,
};
use tokio::sync::watch;
use tokio::time::timeout;

const MAX_TIMING_SAMPLES: usize = 240;
const MAX_DIAGNOSTICS: usize = 128;

#[derive(Debug, Default)]
struct PendingFrame {
    active: bool,
    nodes: BTreeMap<String, UiNode>,
    order: Vec<String>,
    invalid_ids: BTreeSet<String>,
    diagnostics: Vec<SemanticDiagnostic>,
}

#[derive(Debug)]
struct TimingState {
    previous_frame: Option<Instant>,
    prepaint_started: Option<Instant>,
    root_paint_started: Option<Instant>,
    frame_count: u64,
    intervals: VecDeque<Duration>,
    prepaint: VecDeque<Duration>,
    root_paint: VecDeque<Duration>,
}

impl Default for TimingState {
    fn default() -> Self {
        Self {
            previous_frame: None,
            prepaint_started: None,
            root_paint_started: None,
            frame_count: 0,
            intervals: VecDeque::with_capacity(MAX_TIMING_SAMPLES),
            prepaint: VecDeque::with_capacity(MAX_TIMING_SAMPLES),
            root_paint: VecDeque::with_capacity(MAX_TIMING_SAMPLES),
        }
    }
}

#[derive(Debug)]
pub(crate) struct SharedState {
    tree: RwLock<UiTree>,
    pending: Mutex<PendingFrame>,
    highlights: RwLock<Vec<Highlight>>,
    timings: Mutex<TimingState>,
    logs: Mutex<VecDeque<LogEntry>>,
    generation: watch::Sender<u64>,
    completed_frame: watch::Sender<FrameStats>,
    window_geometry: RwLock<Option<WindowGeometry>>,
}

impl SharedState {
    pub(crate) fn new() -> Arc<Self> {
        let (generation, _) = watch::channel(0);
        let (completed_frame, _) = watch::channel(FrameStats::default());
        Arc::new(Self {
            tree: RwLock::new(UiTree::default()),
            pending: Mutex::new(PendingFrame::default()),
            highlights: RwLock::new(Vec::new()),
            timings: Mutex::new(TimingState::default()),
            logs: Mutex::new(VecDeque::with_capacity(512)),
            generation,
            completed_frame,
            window_geometry: RwLock::new(None),
        })
    }

    pub(crate) fn begin_frame(&self) {
        let now = Instant::now();
        {
            let mut timings = self
                .timings
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            timings.frame_count = timings.frame_count.saturating_add(1);
            if let Some(previous) = timings.previous_frame.replace(now) {
                push_sample(
                    &mut timings.intervals,
                    now.saturating_duration_since(previous),
                );
            }
            timings.prepaint_started = Some(now);
            timings.root_paint_started = None;
        }

        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous_was_incomplete = pending.active;
        pending.active = true;
        pending.nodes.clear();
        pending.order.clear();
        pending.invalid_ids.clear();
        pending.diagnostics.clear();
        if previous_was_incomplete {
            push_diagnostic(
                &mut pending,
                SemanticDiagnosticCode::InvalidNode,
                None,
                "the previous semantic frame did not reach root paint",
            );
        }
    }

    pub(crate) fn finish_prepaint(&self) {
        let now = Instant::now();
        let mut timings = self
            .timings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(started) = timings.prepaint_started.take() {
            push_sample(
                &mut timings.prepaint,
                now.saturating_duration_since(started),
            );
        }
    }

    pub(crate) fn begin_root_paint(&self) {
        self.timings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .root_paint_started = Some(Instant::now());
    }

    pub(crate) fn finish_root_paint(&self) {
        let now = Instant::now();
        let stats = {
            let mut timings = self
                .timings
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(started) = timings.root_paint_started.take() {
                push_sample(
                    &mut timings.root_paint,
                    now.saturating_duration_since(started),
                );
            }
            frame_stats_from_timings(&timings)
        };
        self.completed_frame.send_replace(stats);
    }

    pub(crate) fn set_window_geometry(&self, content_bounds: Rect, scale_factor: f32) {
        let geometry = WindowGeometry {
            content_bounds,
            scale_factor,
        };
        if !geometry.is_valid() {
            return;
        }
        *self
            .window_geometry
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(geometry);
    }

    pub(crate) fn window_geometry(&self) -> Option<WindowGeometry> {
        *self
            .window_geometry
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn record(&self, mut node: UiNode) -> bool {
        node.children.clear();

        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !pending.active {
            return false;
        }
        if let Err(message) = validate_node(&node) {
            let node_id = valid_diagnostic_id(&node.id).then(|| node.id.clone());
            push_diagnostic(
                &mut pending,
                SemanticDiagnosticCode::InvalidNode,
                node_id,
                message,
            );
            return false;
        }
        if pending.invalid_ids.contains(&node.id) {
            return false;
        }
        if pending.nodes.remove(&node.id).is_some() {
            pending.order.retain(|id| id != &node.id);
            pending.invalid_ids.insert(node.id.clone());
            push_diagnostic(
                &mut pending,
                SemanticDiagnosticCode::DuplicateId,
                Some(node.id),
                "every node with this duplicate semantic identifier was omitted",
            );
            return false;
        }
        if pending.nodes.len() >= MAX_TREE_NODES {
            push_diagnostic(
                &mut pending,
                SemanticDiagnosticCode::CapacityExceeded,
                None,
                "semantic tree capacity was exceeded",
            );
            return false;
        }
        pending.order.push(node.id.clone());
        pending.nodes.insert(node.id.clone(), node);
        true
    }

    pub(crate) fn publish_frame(&self, nodes: impl IntoIterator<Item = UiNode>) {
        for node in nodes {
            self.record(node);
        }
        self.finish_prepaint();
        self.finish_frame();
    }

    pub(crate) fn finish_frame(&self) {
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !pending.active {
            return;
        }
        pending.active = false;

        discard_invalid_relationships(&mut pending);
        let roots = build_relationships(&mut pending);
        let nodes = std::mem::take(&mut pending.nodes);
        pending.order.clear();
        pending.invalid_ids.clear();
        let diagnostics = std::mem::take(&mut pending.diagnostics);
        drop(pending);

        let mut tree = self
            .tree
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let changed = tree.roots != roots || tree.nodes != nodes || tree.diagnostics != diagnostics;
        if changed {
            tree.generation = tree.generation.saturating_add(1);
        }
        tree.roots = roots;
        tree.nodes = nodes;
        tree.diagnostics = diagnostics;
        let generation = tree.generation;
        drop(tree);

        if changed {
            self.generation.send_replace(generation);
        }
    }

    pub(crate) fn tree(&self) -> UiTree {
        self.tree
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn tree_generation(&self) -> u64 {
        self.tree
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .generation
    }

    pub(crate) async fn wait_for_tree(
        &self,
        after_generation: u64,
        wait: Duration,
    ) -> Result<UiTree, BridgeError> {
        let current = self.tree();
        if current.generation > after_generation {
            return Ok(current);
        }

        let mut receiver = self.generation.subscribe();
        let changed = async {
            loop {
                if *receiver.borrow_and_update() > after_generation {
                    return Ok(());
                }
                receiver.changed().await.map_err(|_| {
                    BridgeError::new(ErrorCode::Internal, "semantic tree publisher stopped")
                })?;
            }
        };
        timeout(wait, changed)
            .await
            .map_err(|_| BridgeError::new(ErrorCode::Timeout, "semantic tree wait timed out"))??;
        Ok(self.tree())
    }

    pub(crate) async fn wait_for_frame(
        &self,
        after_frame_count: u64,
        wait: Duration,
    ) -> Result<FrameStats, BridgeError> {
        let mut receiver = self.completed_frame.subscribe();
        let current = receiver.borrow_and_update().clone();
        if current.frame_count > after_frame_count {
            return Ok(current);
        }

        let changed = async {
            loop {
                receiver.changed().await.map_err(|_| {
                    BridgeError::new(ErrorCode::Internal, "frame publisher stopped")
                })?;
                let current = receiver.borrow_and_update().clone();
                if current.frame_count > after_frame_count {
                    return Ok(current);
                }
            }
        };
        timeout(wait, changed)
            .await
            .map_err(|_| BridgeError::new(ErrorCode::Timeout, "frame wait timed out"))?
    }

    pub(crate) fn set_highlights(&self, highlights: Vec<Highlight>) {
        *self
            .highlights
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = highlights;
    }

    pub(crate) fn highlights(&self) -> Vec<Highlight> {
        self.highlights
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn frame_stats(&self) -> FrameStats {
        self.completed_frame.borrow().clone()
    }

    pub(crate) fn add_log(&self, level: &str, message: &str) {
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        let mut sanitized = message.replace(['\r', '\n'], " ");
        sanitized.truncate(sanitized.floor_char_boundary(4096));
        let mut logs = self
            .logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if logs.len() == 512 {
            logs.pop_front();
        }
        logs.push_back(LogEntry {
            timestamp_ms,
            level: normalize_level(level).to_owned(),
            message: sanitized,
        });
    }

    pub(crate) fn logs(&self, limit: u16, min_level: Option<&str>) -> Vec<LogEntry> {
        let threshold = min_level.map_or(0, level_rank);
        let take = usize::from(limit.min(512));
        self.logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .rev()
            .filter(|entry| level_rank(&entry.level) >= threshold)
            .take(take)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    pub(crate) fn clear_logs(&self) {
        self.logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }
}

fn validate_node(node: &UiNode) -> Result<(), &'static str> {
    validate_id(&node.id)?;
    if let Some(parent) = &node.parent {
        validate_id(parent)?;
    }
    for text in [node.label.as_deref(), node.description.as_deref()]
        .into_iter()
        .flatten()
    {
        if text.len() > MAX_LABEL_BYTES || text.chars().any(char::is_control) {
            return Err("semantic label or description is invalid or exceeds 4 KiB");
        }
    }
    if node.bounds.is_some_and(|bounds| {
        !bounds.is_valid()
            || bounds.x.abs() > 1_000_000.0
            || bounds.y.abs() > 1_000_000.0
            || bounds.width > 1_000_000.0
            || bounds.height > 1_000_000.0
    }) {
        return Err("semantic bounds are invalid or exceed the coordinate limit");
    }
    if node
        .actions
        .iter()
        .enumerate()
        .any(|(index, action)| node.actions[..index].contains(action))
    {
        return Err("semantic actions contain a duplicate");
    }
    if let Some(text) = &node.text {
        if text.text.len() > MAX_TEXT_BYTES {
            return Err("semantic text exceeds 64 KiB");
        }
        if text.redacted && !text.text.is_empty() {
            return Err("redacted semantic text must not contain a value");
        }
        if text
            .caret
            .is_some_and(|caret| caret > text.text.len() || !text.text.is_char_boundary(caret))
        {
            return Err("semantic text caret is not a valid UTF-8 boundary");
        }
        if text.selection.is_some_and(|range| {
            range.start > range.end
                || range.end > text.text.len()
                || !text.text.is_char_boundary(range.start)
                || !text.text.is_char_boundary(range.end)
        }) {
            return Err("semantic text selection is not a valid UTF-8 range");
        }
    }
    if let Some(value) = &node.value {
        if value.value.len() > MAX_TEXT_BYTES {
            return Err("semantic value exceeds 64 KiB");
        }
        if [value.min, value.max, value.step]
            .into_iter()
            .flatten()
            .any(|number| !number.is_finite())
        {
            return Err("semantic numeric bounds must be finite");
        }
        if value.min.zip(value.max).is_some_and(|(min, max)| min > max) {
            return Err("semantic numeric minimum exceeds its maximum");
        }
        if value.step.is_some_and(|step| step <= 0.0) {
            return Err("semantic numeric step must be positive");
        }
    }
    if node.metadata.len() > MAX_METADATA_FIELDS {
        return Err("semantic metadata exceeds 32 fields");
    }
    if node.metadata.iter().any(|(key, value)| {
        key.is_empty()
            || key.len() > MAX_METADATA_KEY_BYTES
            || key.chars().any(char::is_control)
            || value.len() > MAX_METADATA_VALUE_BYTES
            || value.chars().any(char::is_control)
    }) {
        return Err("semantic metadata contains an invalid or oversized field");
    }
    Ok(())
}

fn validate_id(id: &str) -> Result<(), &'static str> {
    if id.is_empty() || id.len() > MAX_ID_BYTES || id.chars().any(char::is_control) {
        return Err("semantic identifier must contain 1-256 bytes without control characters");
    }
    Ok(())
}

fn valid_diagnostic_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= MAX_ID_BYTES && !id.chars().any(char::is_control)
}

fn push_diagnostic(
    pending: &mut PendingFrame,
    code: SemanticDiagnosticCode,
    node_id: Option<String>,
    message: &'static str,
) {
    if pending.diagnostics.len() < MAX_DIAGNOSTICS {
        pending.diagnostics.push(SemanticDiagnostic {
            code,
            node_id,
            message: message.to_owned(),
        });
    }
}

fn discard_invalid_relationships(pending: &mut PendingFrame) {
    let mut invalid = std::mem::take(&mut pending.invalid_ids);
    discard_missing_parents(pending, &mut invalid);

    for start in pending.order.clone() {
        if invalid.contains(&start) {
            continue;
        }
        let mut path: Vec<String> = Vec::new();
        let mut positions: BTreeMap<String, usize> = BTreeMap::new();
        let mut current = start;
        loop {
            if invalid.contains(&current) {
                break;
            }
            if let Some(cycle_start) = positions.get(&current).copied() {
                for id in &path[cycle_start..] {
                    if invalid.insert(id.clone()) {
                        push_diagnostic(
                            pending,
                            SemanticDiagnosticCode::ParentCycle,
                            Some(id.clone()),
                            "semantic node in a parent cycle was omitted",
                        );
                    }
                }
                break;
            }
            positions.insert(current.clone(), path.len());
            path.push(current.clone());
            let Some(parent) = pending
                .nodes
                .get(&current)
                .and_then(|node| node.parent.clone())
            else {
                break;
            };
            current = parent;
        }
    }

    discard_missing_parents(pending, &mut invalid);
    pending.nodes.retain(|id, _| !invalid.contains(id));
    pending.order.retain(|id| !invalid.contains(id));
}

fn discard_missing_parents(pending: &mut PendingFrame, invalid: &mut BTreeSet<String>) {
    loop {
        let missing = pending
            .order
            .iter()
            .filter(|id| !invalid.contains(*id))
            .filter(|id| {
                pending.nodes[*id].parent.as_ref().is_some_and(|parent| {
                    invalid.contains(parent) || !pending.nodes.contains_key(parent)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        if missing.is_empty() {
            break;
        }
        for id in missing {
            invalid.insert(id.clone());
            push_diagnostic(
                pending,
                SemanticDiagnosticCode::MissingParent,
                Some(id),
                "semantic node whose parent was unavailable was omitted",
            );
        }
    }
}

fn build_relationships(pending: &mut PendingFrame) -> Vec<String> {
    let relationships: Vec<_> = pending
        .order
        .iter()
        .filter_map(|id| {
            pending
                .nodes
                .get(id)
                .map(|node| (id.clone(), node.parent.clone()))
        })
        .collect();
    let mut roots = Vec::new();
    for (child, parent) in relationships {
        if let Some(parent) = parent {
            if let Some(parent_node) = pending.nodes.get_mut(&parent) {
                parent_node.children.push(child);
            }
        } else {
            roots.push(child);
        }
    }
    roots
}

fn push_sample(samples: &mut VecDeque<Duration>, sample: Duration) {
    if samples.len() == MAX_TIMING_SAMPLES {
        samples.pop_front();
    }
    samples.push_back(sample);
}

fn frame_stats_from_timings(timings: &TimingState) -> FrameStats {
    let (frame_interval_average_ms, frame_interval_max_ms) = timing_summary(&timings.intervals);
    let (prepaint_average_ms, prepaint_max_ms) = timing_summary(&timings.prepaint);
    let (root_paint_average_ms, root_paint_max_ms) = timing_summary(&timings.root_paint);
    FrameStats {
        frame_count: timings.frame_count,
        sample_count: u32::try_from(timings.intervals.len()).unwrap_or(u32::MAX),
        frame_interval_average_ms,
        frame_interval_max_ms,
        prepaint_average_ms,
        prepaint_max_ms,
        root_paint_average_ms,
        root_paint_max_ms,
        estimated_fps: if frame_interval_average_ms > 0.0 {
            1000.0 / frame_interval_average_ms
        } else {
            0.0
        },
    }
}

#[allow(clippy::cast_precision_loss)]
fn timing_summary(samples: &VecDeque<Duration>) -> (f64, f64) {
    if samples.is_empty() {
        return (0.0, 0.0);
    }
    let total_ms = samples.iter().map(Duration::as_secs_f64).sum::<f64>() * 1000.0;
    let average_ms = total_ms / samples.len() as f64;
    let max_ms = samples
        .iter()
        .map(|duration| duration.as_secs_f64() * 1000.0)
        .fold(0.0, f64::max);
    (average_ms, max_ms)
}

fn normalize_level(level: &str) -> &'static str {
    match level.to_ascii_lowercase().as_str() {
        "trace" => "trace",
        "debug" => "debug",
        "warn" | "warning" => "warn",
        "error" => "error",
        _ => "info",
    }
}

fn level_rank(level: &str) -> u8 {
    match normalize_level(level) {
        "trace" => 0,
        "debug" => 1,
        "warn" => 3,
        "error" => 4,
        _ => 2,
    }
}

pub(crate) fn rect_from_gpui(bounds: gpui::Bounds<gpui::Pixels>) -> Rect {
    Rect {
        x: f32::from(bounds.origin.x),
        y: f32::from(bounds.origin.y),
        width: f32::from(bounds.size.width),
        height: f32::from(bounds.size.height),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use gpui_mcp_protocol::{NodeState, Role, SemanticDiagnosticCode, UiNode};

    use super::SharedState;

    fn node(id: &str, parent: Option<&str>) -> UiNode {
        UiNode {
            id: id.to_owned(),
            parent: parent.map(str::to_owned),
            children: Vec::new(),
            role: Role::Generic,
            label: None,
            description: None,
            bounds: None,
            state: NodeState::default(),
            actions: Vec::new(),
            text: None,
            value: None,
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn duplicate_ids_are_omitted_and_reported() {
        let state = SharedState::new();
        state.begin_frame();
        assert!(state.record(node("same", None)));
        assert!(!state.record(node("same", None)));
        state.finish_frame();

        let tree = state.tree();
        assert!(tree.nodes.is_empty());
        assert!(tree.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == SemanticDiagnosticCode::DuplicateId
                && diagnostic.node_id.as_deref() == Some("same")
        }));
    }

    #[test]
    fn missing_parents_and_cycles_are_rejected_without_rewriting_the_graph() {
        let state = SharedState::new();
        state.begin_frame();
        assert!(state.record(node("missing", Some("absent"))));
        assert!(state.record(node("a", Some("b"))));
        assert!(state.record(node("b", Some("a"))));
        state.finish_frame();

        let tree = state.tree();
        assert!(tree.nodes.is_empty());
        assert!(tree.roots.is_empty());
        assert!(
            tree.diagnostics
                .iter()
                .any(|diagnostic| { diagnostic.code == SemanticDiagnosticCode::MissingParent })
        );
        assert!(
            tree.diagnostics
                .iter()
                .any(|diagnostic| { diagnostic.code == SemanticDiagnosticCode::ParentCycle })
        );
    }

    #[test]
    fn unchanged_semantics_do_not_advance_generation() {
        let state = SharedState::new();
        for _ in 0..2 {
            state.begin_frame();
            assert!(state.record(node("stable", None)));
            state.finish_frame();
        }
        assert_eq!(state.tree().generation, 1);
    }

    #[tokio::test]
    async fn tree_wait_wakes_when_a_new_generation_is_published() -> Result<(), String> {
        let state = SharedState::new();
        let waiter_state = state.clone();
        let waiter =
            tokio::spawn(
                async move { waiter_state.wait_for_tree(0, Duration::from_secs(1)).await },
            );
        tokio::task::yield_now().await;

        state.begin_frame();
        assert!(state.record(node("published", None)));
        state.finish_frame();

        let tree = waiter
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.message)?;
        assert_eq!(tree.generation, 1);
        assert!(tree.nodes.contains_key("published"));
        Ok(())
    }

    #[tokio::test]
    async fn frame_wait_wakes_after_unchanged_semantics_finish_paint() -> Result<(), String> {
        let state = SharedState::new();
        state.begin_frame();
        assert!(state.record(node("stable", None)));
        state.finish_frame();
        state.begin_root_paint();
        state.finish_root_paint();
        assert_eq!(state.tree().generation, 1);

        let waiter_state = state.clone();
        let waiter =
            tokio::spawn(
                async move { waiter_state.wait_for_frame(1, Duration::from_secs(1)).await },
            );
        tokio::task::yield_now().await;

        state.begin_frame();
        assert!(state.record(node("stable", None)));
        state.finish_frame();
        state.begin_root_paint();
        state.finish_root_paint();

        let observed_frame = waiter
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.message)?;
        assert_eq!(observed_frame.frame_count, 2);
        assert_eq!(state.tree().generation, 1);
        Ok(())
    }

    #[tokio::test]
    async fn frame_wait_never_returns_a_started_but_incomplete_frame() -> Result<(), String> {
        let state = SharedState::new();
        state.begin_frame();
        assert!(state.record(node("stable", None)));
        state.finish_frame();
        state.begin_root_paint();
        state.finish_root_paint();

        state.begin_frame();
        assert!(state.record(node("stable", None)));
        state.finish_frame();
        state.begin_root_paint();
        assert_eq!(state.frame_stats().frame_count, 1);

        let wait = state.wait_for_frame(1, Duration::from_secs(1));
        tokio::pin!(wait);
        tokio::select! {
            biased;
            result = &mut wait => {
                return Err(format!(
                    "frame wait completed before root paint: {:?}",
                    result.map_err(|error| error.message)
                ));
            }
            () = tokio::task::yield_now() => {}
        }

        state.finish_root_paint();
        let observed = wait.await.map_err(|error| error.message)?;
        assert_eq!(observed.frame_count, 2);
        assert_eq!(state.frame_stats(), observed);
        Ok(())
    }
}
