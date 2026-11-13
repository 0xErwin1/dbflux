use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use gpui::AccessibilityFrame;
use gpui_mcp_protocol::{
    Annotation, AnnotationSource, AnnotationSpec, BridgeError, Distribution, ErrorCode,
    FrameReport, FrameSample, FrameStats, FrameSummary, Highlight, LogEntry, MAX_FRAME_SAMPLES,
    MAX_FRAME_VIEWS, MAX_ID_BYTES, MAX_LABEL_BYTES, MAX_METADATA_FIELDS, MAX_METADATA_KEY_BYTES,
    MAX_METADATA_VALUE_BYTES, MAX_TEXT_BYTES, MAX_TREE_NODES, Message, MessagePage, MessageSender,
    NewMessage, Rect, SemanticDiagnostic, SemanticDiagnosticCode, UiNode, UiTree, ViewActivity,
    ViewDraw, ViewOutcome, ViewRenderCause, WindowGeometry,
};
use tokio::sync::watch;
use tokio::time::timeout;

use crate::annotations::{AnnotationStore, HIGHLIGHT_GROUP, PaintItem};
use crate::messages::MessageLog;

const MAX_TIMING_SAMPLES: usize = 240;
const MAX_DIAGNOSTICS: usize = 128;

/// The newest accessibility frame GPUI handed over, not yet converted.
///
/// Converting a frame into a [`UiTree`] costs time in proportion to the tree, so
/// the draw only keeps GPUI's shared handle and the conversion runs when a
/// client reads the tree, on the thread that reads it. An unread frame is simply
/// replaced by the next one.
#[derive(Default)]
struct SemanticIntake {
    pending: Option<PendingSemantics>,
    /// A frame started and has not delivered its accessibility tree yet.
    awaiting: bool,
    /// A started frame never delivered its accessibility tree.
    incomplete: bool,
}

struct PendingSemantics {
    frame: Arc<AccessibilityFrame>,
    /// Whether a frame since the last converted tree failed to deliver one.
    incomplete: bool,
}

/// Nodes of one frame being validated into a tree.
#[derive(Debug, Default)]
struct TreeBuilder {
    nodes: BTreeMap<String, UiNode>,
    order: Vec<String>,
    invalid_ids: BTreeSet<String>,
    diagnostics: Vec<SemanticDiagnostic>,
}

/// One view drawn in a retained frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ViewRecord {
    pub(crate) entity_id: u64,
    pub(crate) type_name: &'static str,
    /// Why the view rendered, or `None` when it replayed from cache.
    pub(crate) cause: Option<ViewRenderCause>,
}

/// The frame GPUI is drawing now.
#[derive(Debug)]
struct FrameInProgress {
    started: Instant,
    interval: Option<Duration>,
    prepaint: Option<Duration>,
    root_paint_started: Option<Instant>,
    root_paint: Option<Duration>,
}

/// One completed frame, retained for reports.
#[derive(Clone, Debug)]
struct FrameRecord {
    frame_count: u64,
    interval: Option<Duration>,
    draw: Duration,
    prepaint: Duration,
    root_paint: Duration,
    bridge: Duration,
    views_rendered: u32,
    views_reused: u32,
    views: Arc<[ViewRecord]>,
}

#[derive(Debug)]
struct TimingState {
    previous_frame: Option<Instant>,
    current: Option<FrameInProgress>,
    frame_count: u64,
    mark: u64,
    intervals: VecDeque<Duration>,
    prepaint: VecDeque<Duration>,
    root_paint: VecDeque<Duration>,
    draw: VecDeque<Duration>,
    bridge: VecDeque<Duration>,
    history: VecDeque<FrameRecord>,
}

impl Default for TimingState {
    fn default() -> Self {
        Self {
            previous_frame: None,
            current: None,
            frame_count: 0,
            mark: 0,
            intervals: VecDeque::with_capacity(MAX_TIMING_SAMPLES),
            prepaint: VecDeque::with_capacity(MAX_TIMING_SAMPLES),
            root_paint: VecDeque::with_capacity(MAX_TIMING_SAMPLES),
            draw: VecDeque::with_capacity(MAX_TIMING_SAMPLES),
            bridge: VecDeque::with_capacity(MAX_TIMING_SAMPLES),
            history: VecDeque::with_capacity(MAX_FRAME_SAMPLES),
        }
    }
}

impl TimingState {
    fn clear_samples(&mut self) {
        self.intervals.clear();
        self.prepaint.clear();
        self.root_paint.clear();
        self.draw.clear();
        self.bridge.clear();
    }
}

#[derive(Debug)]
pub(crate) struct SharedState {
    tree: RwLock<UiTree>,
    intake: Mutex<SemanticIntake>,
    /// Serializes conversions so two readers never convert one frame twice.
    converting: Mutex<()>,
    /// Counts accessibility frames handed over, to wake tree waiters.
    semantic_frames: watch::Sender<u64>,
    annotations: Mutex<AnnotationStore>,
    /// Number of annotations, read on every frame without taking the lock.
    annotation_count: AtomicUsize,
    annotation_changes: watch::Sender<AnnotationChange>,
    /// The latest accessibility frame, kept only while annotations need it.
    overlay_frame: Mutex<Option<Arc<AccessibilityFrame>>>,
    paint_annotations: AtomicBool,
    /// Deadline of the scheduled expiry frame, if one is scheduled.
    expiry_timer: Mutex<Option<Instant>>,
    messages: Mutex<MessageLog>,
    /// Id of the newest message, to wake readers.
    message_events: watch::Sender<u64>,
    accepts_agent_messages: AtomicBool,
    timings: Mutex<TimingState>,
    logs: Mutex<VecDeque<LogEntry>>,
    completed_frame: watch::Sender<FrameStats>,
    presented_frame: watch::Sender<u64>,
    window_geometry: RwLock<Option<WindowGeometry>>,
}

impl std::fmt::Debug for SemanticIntake {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SemanticIntake")
            .field("pending", &self.pending.is_some())
            .field("awaiting", &self.awaiting)
            .field("incomplete", &self.incomplete)
            .finish()
    }
}

impl SharedState {
    pub(crate) fn new() -> Arc<Self> {
        let (semantic_frames, _) = watch::channel(0);
        let (completed_frame, _) = watch::channel(FrameStats::default());
        let (presented_frame, _) = watch::channel(0);
        Arc::new(Self {
            tree: RwLock::new(UiTree::default()),
            intake: Mutex::new(SemanticIntake::default()),
            converting: Mutex::new(()),
            semantic_frames,
            annotations: Mutex::new(AnnotationStore::default()),
            annotation_count: AtomicUsize::new(0),
            annotation_changes: watch::channel(AnnotationChange::default()).0,
            overlay_frame: Mutex::new(None),
            paint_annotations: AtomicBool::new(true),
            expiry_timer: Mutex::new(None),
            messages: Mutex::new(MessageLog::default()),
            message_events: watch::channel(0).0,
            accepts_agent_messages: AtomicBool::new(false),
            timings: Mutex::new(TimingState::default()),
            logs: Mutex::new(VecDeque::with_capacity(512)),
            completed_frame,
            presented_frame,
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
            let interval = timings
                .previous_frame
                .replace(now)
                .map(|previous| now.saturating_duration_since(previous));
            if let Some(interval) = interval {
                push_sample(&mut timings.intervals, interval);
            }
            timings.current = Some(FrameInProgress {
                started: now,
                interval,
                prepaint: None,
                root_paint_started: None,
                root_paint: None,
            });
        }

        let mut intake = self
            .intake
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if intake.awaiting {
            intake.incomplete = true;
        }
        intake.awaiting = true;
    }

    /// Keep GPUI's finished accessibility frame for conversion when it is read.
    pub(crate) fn observe_semantics(&self, frame: &Arc<AccessibilityFrame>) {
        let now = Instant::now();
        {
            let mut timings = self
                .timings
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(current) = timings.current.as_mut() {
                current.prepaint = Some(now.saturating_duration_since(current.started));
            }
        }

        let replaced = {
            let mut intake = self
                .intake
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            intake.awaiting = false;
            let carried = intake
                .pending
                .as_ref()
                .is_some_and(|pending| pending.incomplete);
            let incomplete = std::mem::take(&mut intake.incomplete) || carried;
            intake.pending.replace(PendingSemantics {
                frame: frame.clone(),
                incomplete,
            })
        };
        // Release the lock before an unread frame is freed.
        drop(replaced);
        self.semantic_frames.send_modify(|count| *count += 1);
    }

    pub(crate) fn begin_root_paint(&self) {
        let mut timings = self
            .timings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(current) = timings.current.as_mut() {
            current.root_paint_started = Some(Instant::now());
        }
    }

    pub(crate) fn finish_root_paint(&self) {
        let now = Instant::now();
        let mut timings = self
            .timings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(current) = timings.current.as_mut()
            && let Some(started) = current.root_paint_started.take()
        {
            current.root_paint = Some(now.saturating_duration_since(started));
        }
    }

    /// Complete the current frame with GPUI's measurement of the whole draw and
    /// what each view did, then publish it to frame waiters.
    pub(crate) fn finish_draw(
        &self,
        draw: Duration,
        bridge: Duration,
        views: impl IntoIterator<Item = ViewRecord>,
    ) {
        let mut views_rendered = 0_u32;
        let mut views_reused = 0_u32;
        let mut itemized = Vec::new();
        for view in views {
            if view.cause.is_some() {
                views_rendered = views_rendered.saturating_add(1);
            } else {
                views_reused = views_reused.saturating_add(1);
            }
            if itemized.len() < MAX_FRAME_VIEWS {
                itemized.push(view);
            }
        }

        let stats = {
            let mut timings = self
                .timings
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let current = timings.current.take();
            timings.frame_count = timings.frame_count.saturating_add(1);
            let prepaint = current
                .as_ref()
                .and_then(|current| current.prepaint)
                .unwrap_or_default();
            let root_paint = current
                .as_ref()
                .and_then(|current| current.root_paint)
                .unwrap_or_default();
            push_sample(&mut timings.prepaint, prepaint);
            push_sample(&mut timings.root_paint, root_paint);
            push_sample(&mut timings.draw, draw);
            push_sample(&mut timings.bridge, bridge);
            if timings.history.len() == MAX_FRAME_SAMPLES {
                timings.history.pop_front();
            }
            let record = FrameRecord {
                frame_count: timings.frame_count,
                interval: current.and_then(|current| current.interval),
                draw,
                prepaint,
                root_paint,
                bridge,
                views_rendered,
                views_reused,
                views: itemized.into(),
            };
            timings.history.push_back(record);
            frame_stats_from_timings(&timings)
        };
        self.completed_frame.send_replace(stats);
    }

    /// Start a measurement window at the last completed frame.
    pub(crate) fn mark_frames(&self) -> FrameStats {
        let stats = {
            let mut timings = self
                .timings
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            timings.mark = timings.frame_count;
            timings.clear_samples();
            frame_stats_from_timings(&timings)
        };
        self.completed_frame.send_replace(stats.clone());
        stats
    }

    /// Report the retained frames completed after `after_frame_count`, or after
    /// the last mark, returning at most `frame_limit` per-frame samples.
    pub(crate) fn frame_report(
        &self,
        after_frame_count: Option<u64>,
        frame_limit: usize,
    ) -> FrameReport {
        let (after_frame_count, latest_frame_count, records) = {
            let timings = self
                .timings
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let after = after_frame_count.unwrap_or(timings.mark);
            let records = timings
                .history
                .iter()
                .filter(|record| record.frame_count > after)
                .cloned()
                .collect::<Vec<_>>();
            (after, timings.frame_count, records)
        };
        build_report(after_frame_count, latest_frame_count, &records, frame_limit)
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

    /// Convert the newest handed-over accessibility frame, if it has not been.
    fn convert_pending(&self) {
        let _converting = self
            .converting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pending = self
            .intake
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending
            .take();
        let Some(pending) = pending else {
            return;
        };
        let nodes = crate::observer::semantic_nodes(&pending.frame);
        drop(pending.frame);
        self.publish_nodes(nodes, pending.incomplete);
    }

    /// Validate one frame's nodes and publish them as the current tree,
    /// advancing the generation only when the tree changed.
    fn publish_nodes(&self, nodes: impl IntoIterator<Item = UiNode>, incomplete: bool) {
        let mut builder = TreeBuilder::default();
        if incomplete {
            builder.push_diagnostic(
                SemanticDiagnosticCode::InvalidNode,
                None,
                "a semantic frame since the previous tree did not reach root paint",
            );
        }
        for node in nodes {
            builder.record(node);
        }
        let (roots, nodes, diagnostics) = builder.finish();

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
    }

    pub(crate) fn tree(&self) -> UiTree {
        self.convert_pending();
        self.tree
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn tree_generation(&self) -> u64 {
        self.convert_pending();
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
        let mut receiver = self.semantic_frames.subscribe();
        let changed = async {
            loop {
                // Mark the current frame seen before converting it, so a frame
                // handed over during the conversion still wakes the wait below.
                receiver.borrow_and_update();
                let current = self.tree();
                if current.generation > after_generation {
                    return Ok(current);
                }
                receiver.changed().await.map_err(|_| {
                    BridgeError::new(ErrorCode::Internal, "semantic tree publisher stopped")
                })?;
            }
        };
        timeout(wait, changed)
            .await
            .map_err(|_| BridgeError::new(ErrorCode::Timeout, "semantic tree wait timed out"))?
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

    /// Record that every frame completed so far has been presented to the platform window.
    ///
    /// Called from a GPUI next-frame callback. GPUI runs those at the start of a frame
    /// request, before the request draws, and every earlier request presented the frame it
    /// drew before returning, so each completed root-paint frame has been handed to the
    /// platform window by then.
    pub(crate) fn mark_frames_presented(&self) {
        let completed = self.completed_frame.borrow().frame_count;
        self.presented_frame.send_if_modified(|presented| {
            if completed > *presented {
                *presented = completed;
                true
            } else {
                false
            }
        });
    }

    #[cfg(test)]
    pub(crate) fn presented_frame_count(&self) -> u64 {
        *self.presented_frame.borrow()
    }

    /// Wait until a frame newer than `after_frame_count` has been presented, then return the
    /// current frame statistics.
    ///
    /// Only [`Self::mark_frames_presented`] advances the presented-frame token, so the caller
    /// must have scheduled it, as `Operation::Refresh` and `Operation::RequestFrame` do.
    pub(crate) async fn wait_for_presented_frame(
        &self,
        after_frame_count: u64,
        wait: Duration,
    ) -> Result<FrameStats, BridgeError> {
        let mut receiver = self.presented_frame.subscribe();
        let presented = async {
            loop {
                if *receiver.borrow_and_update() > after_frame_count {
                    return Ok(());
                }
                receiver.changed().await.map_err(|_| {
                    BridgeError::new(ErrorCode::Internal, "frame publisher stopped")
                })?;
            }
        };
        timeout(wait, presented).await.map_err(|_| {
            BridgeError::new(ErrorCode::Timeout, "presented frame wait timed out")
        })??;
        Ok(self.frame_stats())
    }

    /// Show legacy highlights as fixed-rectangle annotations in their own group.
    pub(crate) fn set_highlights(&self, highlights: Vec<Highlight>) {
        let specs = highlights
            .into_iter()
            .enumerate()
            .map(|(index, highlight)| {
                let mut spec = AnnotationSpec::rect(highlight.rect)
                    .with_id(format!("highlight-{index}"))
                    .with_color(highlight.color)
                    .with_group(HIGHLIGHT_GROUP);
                spec.label = highlight.label;
                spec
            })
            .collect();
        if let Err(error) =
            self.upsert_annotations(specs, Some(HIGHLIGHT_GROUP), AnnotationSource::Agent)
        {
            tracing::warn!(message = %error.message, "could not show highlights");
        }
    }

    fn with_annotations<R>(
        &self,
        source: AnnotationSource,
        change: impl FnOnce(&mut AnnotationStore) -> R,
        changed: impl FnOnce(&R) -> bool,
    ) -> R {
        let mut store = self
            .annotations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = change(&mut store);
        if changed(&result) {
            self.annotation_count.store(store.len(), Ordering::Release);
            drop(store);
            self.annotation_changes.send_modify(|current| {
                current.revision += 1;
                current.source = source;
            });
        }
        result
    }

    pub(crate) fn upsert_annotations(
        &self,
        specs: Vec<AnnotationSpec>,
        replace_group: Option<&str>,
        source: AnnotationSource,
    ) -> Result<Vec<Annotation>, BridgeError> {
        self.with_annotations(
            source,
            |store| store.upsert(specs, replace_group, source, unix_ms()),
            Result::is_ok,
        )
    }

    pub(crate) fn remove_annotations(
        &self,
        ids: &[impl AsRef<str>],
        source: AnnotationSource,
    ) -> usize {
        self.with_annotations(source, |store| store.remove(ids), |removed| *removed > 0)
    }

    pub(crate) fn clear_annotations(&self, group: Option<&str>, source: AnnotationSource) -> usize {
        self.with_annotations(source, |store| store.clear(group), |removed| *removed > 0)
    }

    pub(crate) fn annotations(&self) -> Vec<Annotation> {
        self.annotations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .list()
    }

    pub(crate) fn subscribe_annotations(&self) -> watch::Receiver<AnnotationChange> {
        self.annotation_changes.subscribe()
    }

    /// Wake annotation listeners without changing anything.
    pub(crate) fn notify_annotation_listeners(&self) {
        self.annotation_changes.send_modify(|_| {});
    }

    pub(crate) fn annotation_revision(&self) -> u64 {
        self.annotation_changes.borrow().revision
    }

    pub(crate) fn set_paint_annotations(&self, paint: bool) {
        self.paint_annotations.store(paint, Ordering::Release);
    }

    pub(crate) fn paints_annotations(&self) -> bool {
        self.paint_annotations.load(Ordering::Acquire)
    }

    /// Whether the overlay pass needs this frame's accessibility tree.
    pub(crate) fn has_annotations(&self) -> bool {
        self.annotation_count.load(Ordering::Acquire) > 0
    }

    /// Keep the frame for resolving annotation targets, or release a kept one.
    pub(crate) fn keep_overlay_frame(&self, frame: Option<&Arc<AccessibilityFrame>>) {
        let replaced = std::mem::replace(
            &mut *self
                .overlay_frame
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            frame.cloned(),
        );
        drop(replaced);
    }

    /// Drop expired annotations, then return what to draw this frame, the
    /// frame to resolve node targets against, and the next expiry.
    pub(crate) fn annotation_paint(
        &self,
        now: Instant,
    ) -> (
        Vec<PaintItem>,
        Option<Arc<AccessibilityFrame>>,
        Option<Instant>,
    ) {
        let (items, next_expiry, _) = self.with_annotations(
            AnnotationSource::Bridge,
            |store| {
                let pruned = store.prune_expired(now);
                (store.paint_items(), store.next_expiry(), pruned)
            },
            |(_, _, pruned)| *pruned,
        );
        let frame = self
            .overlay_frame
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        (items, frame, next_expiry)
    }

    pub(crate) fn record_resolved(&self, resolved: &BTreeMap<String, Option<Rect>>) {
        self.annotations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .set_resolved(resolved);
    }

    /// Claim the expiry frame for `deadline`, unless an earlier one is scheduled.
    pub(crate) fn claim_expiry_timer(&self, deadline: Instant) -> bool {
        let mut scheduled = self
            .expiry_timer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if scheduled.is_some_and(|current| current <= deadline) {
            return false;
        }
        *scheduled = Some(deadline);
        true
    }

    pub(crate) fn expiry_timer_fired(&self, deadline: Instant) {
        let mut scheduled = self
            .expiry_timer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *scheduled == Some(deadline) {
            *scheduled = None;
        }
    }

    pub(crate) fn post_message(
        &self,
        from: MessageSender,
        message: NewMessage,
    ) -> Result<Message, BridgeError> {
        let posted = self
            .messages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .post(from, message, unix_ms())?;
        self.message_events.send_replace(posted.id);
        Ok(posted)
    }

    pub(crate) fn read_messages(
        &self,
        after: u64,
        from: Option<MessageSender>,
        limit: usize,
        reader: Option<MessageSender>,
    ) -> MessagePage {
        let page = self
            .messages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .read(after, from, limit, reader);
        if reader.is_some() {
            // Readers can free room for a blocked writer; wake waiters to recount.
            self.message_events.send_modify(|_| {});
        }
        page
    }

    pub(crate) fn mark_messages_read(&self, reader: MessageSender, id: u64) {
        self.messages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .mark_read(reader, id);
    }

    pub(crate) fn set_accepts_agent_messages(&self, accepts: bool) {
        self.accepts_agent_messages
            .store(accepts, Ordering::Release);
    }

    pub(crate) fn accepts_agent_messages(&self) -> bool {
        self.accepts_agent_messages.load(Ordering::Acquire)
    }

    /// Wake message readers without posting anything.
    pub(crate) fn wake_message_readers(&self) {
        self.message_events.send_modify(|_| {});
    }

    pub(crate) fn subscribe_messages(&self) -> watch::Receiver<u64> {
        self.message_events.subscribe()
    }

    /// Read messages after `after`, waiting up to `wait` for a matching one.
    /// A wait that times out returns an empty page.
    pub(crate) async fn wait_for_messages(
        &self,
        after: u64,
        from: Option<MessageSender>,
        limit: usize,
        wait: Duration,
        reader: Option<MessageSender>,
    ) -> MessagePage {
        let mut receiver = self.message_events.subscribe();
        let arrived = async {
            loop {
                receiver.borrow_and_update();
                let available = self
                    .messages
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .has_after(after, from);
                if available || receiver.changed().await.is_err() {
                    return;
                }
            }
        };
        let _ = timeout(wait, arrived).await;
        self.read_messages(after, from, limit, reader)
    }

    pub(crate) fn frame_stats(&self) -> FrameStats {
        self.completed_frame.borrow().clone()
    }

    pub(crate) fn add_log(&self, level: &str, message: &str) {
        let timestamp_ms = unix_ms();
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

impl TreeBuilder {
    fn record(&mut self, mut node: UiNode) -> bool {
        node.children.clear();

        if let Err(message) = validate_node(&node) {
            let node_id = valid_diagnostic_id(&node.id).then(|| node.id.clone());
            self.push_diagnostic(SemanticDiagnosticCode::InvalidNode, node_id, message);
            return false;
        }
        if self.invalid_ids.contains(&node.id) {
            return false;
        }
        if self.nodes.remove(&node.id).is_some() {
            self.order.retain(|id| id != &node.id);
            self.invalid_ids.insert(node.id.clone());
            self.push_diagnostic(
                SemanticDiagnosticCode::DuplicateId,
                Some(node.id),
                "every node with this duplicate semantic identifier was omitted",
            );
            return false;
        }
        if self.nodes.len() >= MAX_TREE_NODES {
            self.push_diagnostic(
                SemanticDiagnosticCode::CapacityExceeded,
                None,
                "semantic tree capacity was exceeded",
            );
            return false;
        }
        self.order.push(node.id.clone());
        self.nodes.insert(node.id.clone(), node);
        true
    }

    fn finish(
        mut self,
    ) -> (
        Vec<String>,
        BTreeMap<String, UiNode>,
        Vec<SemanticDiagnostic>,
    ) {
        discard_invalid_relationships(&mut self);
        let roots = build_relationships(&mut self);
        (roots, self.nodes, self.diagnostics)
    }

    fn push_diagnostic(
        &mut self,
        code: SemanticDiagnosticCode,
        node_id: Option<String>,
        message: &'static str,
    ) {
        if self.diagnostics.len() < MAX_DIAGNOSTICS {
            self.diagnostics.push(SemanticDiagnostic {
                code,
                node_id,
                message: message.to_owned(),
            });
        }
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

fn discard_invalid_relationships(builder: &mut TreeBuilder) {
    let mut invalid = std::mem::take(&mut builder.invalid_ids);
    discard_missing_parents(builder, &mut invalid);

    for start in builder.order.clone() {
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
                        builder.push_diagnostic(
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
            let Some(parent) = builder
                .nodes
                .get(&current)
                .and_then(|node| node.parent.clone())
            else {
                break;
            };
            current = parent;
        }
    }

    discard_missing_parents(builder, &mut invalid);
    builder.nodes.retain(|id, _| !invalid.contains(id));
    builder.order.retain(|id| !invalid.contains(id));
}

fn discard_missing_parents(builder: &mut TreeBuilder, invalid: &mut BTreeSet<String>) {
    loop {
        let missing = builder
            .order
            .iter()
            .filter(|id| !invalid.contains(*id))
            .filter(|id| {
                builder.nodes[*id].parent.as_ref().is_some_and(|parent| {
                    invalid.contains(parent) || !builder.nodes.contains_key(parent)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        if missing.is_empty() {
            break;
        }
        for id in missing {
            invalid.insert(id.clone());
            builder.push_diagnostic(
                SemanticDiagnosticCode::MissingParent,
                Some(id),
                "semantic node whose parent was unavailable was omitted",
            );
        }
    }
}

fn build_relationships(builder: &mut TreeBuilder) -> Vec<String> {
    let relationships: Vec<_> = builder
        .order
        .iter()
        .filter_map(|id| {
            builder
                .nodes
                .get(id)
                .map(|node| (id.clone(), node.parent.clone()))
        })
        .collect();
    let mut roots = Vec::new();
    for (child, parent) in relationships {
        if let Some(parent) = parent {
            if let Some(parent_node) = builder.nodes.get_mut(&parent) {
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
    let (draw_average_ms, draw_max_ms) = timing_summary(&timings.draw);
    let (bridge_average_ms, bridge_max_ms) = timing_summary(&timings.bridge);
    FrameStats {
        frame_count: timings.frame_count,
        sample_count: u32::try_from(timings.intervals.len()).unwrap_or(u32::MAX),
        frame_interval_average_ms,
        frame_interval_max_ms,
        prepaint_average_ms,
        prepaint_max_ms,
        root_paint_average_ms,
        root_paint_max_ms,
        draw_average_ms,
        draw_max_ms,
        bridge_average_ms,
        bridge_max_ms,
        mark_frame_count: timings.mark,
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

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

/// Mean and nearest-rank percentiles of `values`.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn distribution(mut values: Vec<f64>) -> Distribution {
    if values.is_empty() {
        return Distribution::default();
    }
    values.sort_by(f64::total_cmp);
    let count = values.len();
    let rank = |quantile: f64| {
        let position = (quantile * count as f64).ceil() as usize;
        values[position.clamp(1, count) - 1]
    };
    Distribution {
        count: u32::try_from(count).unwrap_or(u32::MAX),
        mean: values.iter().sum::<f64>() / count as f64,
        p50: rank(0.5),
        p95: rank(0.95),
        max: values[count - 1],
    }
}

fn view_draw(view: &ViewRecord) -> ViewDraw {
    ViewDraw {
        entity_id: view.entity_id,
        type_name: view.type_name.to_owned(),
        outcome: if view.cause.is_some() {
            ViewOutcome::Rendered
        } else {
            ViewOutcome::Reused
        },
        cause: view.cause,
    }
}

fn frame_sample(record: &FrameRecord) -> FrameSample {
    FrameSample {
        frame_count: record.frame_count,
        interval_ms: record.interval.map(milliseconds),
        draw_ms: milliseconds(record.draw),
        app_draw_ms: milliseconds(record.draw.saturating_sub(record.bridge)),
        prepaint_ms: milliseconds(record.prepaint),
        root_paint_ms: milliseconds(record.root_paint),
        bridge_ms: milliseconds(record.bridge),
        views_rendered: record.views_rendered,
        views_reused: record.views_reused,
        rendered_views: record
            .views
            .iter()
            .filter(|view| view.cause.is_some())
            .map(view_draw)
            .collect(),
    }
}

fn build_report(
    after_frame_count: u64,
    latest_frame_count: u64,
    records: &[FrameRecord],
    frame_limit: usize,
) -> FrameReport {
    // Every completed frame is retained in order, so a gap between the mark
    // and the oldest retained frame means frames were evicted.
    let truncated = records
        .first()
        .map_or(latest_frame_count > after_frame_count, |first| {
            first.frame_count > after_frame_count.saturating_add(1)
        });

    let samples = records.iter().map(frame_sample).collect::<Vec<_>>();
    let summary = FrameSummary {
        frames: u32::try_from(samples.len()).unwrap_or(u32::MAX),
        draw_ms: distribution(samples.iter().map(|sample| sample.draw_ms).collect()),
        app_draw_ms: distribution(samples.iter().map(|sample| sample.app_draw_ms).collect()),
        prepaint_ms: distribution(samples.iter().map(|sample| sample.prepaint_ms).collect()),
        root_paint_ms: distribution(samples.iter().map(|sample| sample.root_paint_ms).collect()),
        bridge_ms: distribution(samples.iter().map(|sample| sample.bridge_ms).collect()),
        interval_ms: distribution(
            samples
                .iter()
                .filter_map(|sample| sample.interval_ms)
                .collect(),
        ),
        views_rendered: samples
            .iter()
            .map(|sample| u64::from(sample.views_rendered))
            .sum(),
        views_reused: samples
            .iter()
            .map(|sample| u64::from(sample.views_reused))
            .sum(),
    };

    let mut positions = HashMap::<u64, usize>::new();
    let mut views = Vec::<ViewActivity>::new();
    for view in records.iter().flat_map(|record| record.views.iter()) {
        let position = *positions.entry(view.entity_id).or_insert_with(|| {
            views.push(ViewActivity {
                entity_id: view.entity_id,
                ..ViewActivity::default()
            });
            views.len() - 1
        });
        let activity = &mut views[position];
        if activity.type_name.is_empty() {
            view.type_name.clone_into(&mut activity.type_name);
        }
        if let Some(cause) = view.cause {
            activity.rendered = activity.rendered.saturating_add(1);
            let count = activity.causes.entry(cause).or_default();
            *count = count.saturating_add(1);
        } else {
            activity.reused = activity.reused.saturating_add(1);
        }
    }
    views.sort_by(|left, right| {
        right
            .rendered
            .cmp(&left.rendered)
            .then(right.reused.cmp(&left.reused))
            .then(left.entity_id.cmp(&right.entity_id))
    });

    let last_frame_views = records
        .last()
        .map(|record| record.views.iter().map(view_draw).collect())
        .unwrap_or_default();
    let skip = samples.len().saturating_sub(frame_limit);
    FrameReport {
        after_frame_count,
        latest_frame_count,
        truncated,
        summary,
        frames: samples.into_iter().skip(skip).collect(),
        views,
        last_frame_views,
    }
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

/// The latest change to the annotation set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AnnotationChange {
    /// Increases with every change.
    pub(crate) revision: u64,
    /// Who made the latest change.
    pub(crate) source: AnnotationSource,
}

impl Default for AnnotationChange {
    fn default() -> Self {
        Self {
            revision: 0,
            source: AnnotationSource::Bridge,
        }
    }
}

pub(crate) fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
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

    use gpui_mcp_protocol::{
        ErrorCode, NodeState, Role, SemanticDiagnosticCode, UiNode, ViewRenderCause,
    };

    use super::{SharedState, TreeBuilder, ViewRecord};

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

    /// Complete one frame the way the observer does, without semantics.
    fn draw_frame(state: &SharedState, draw_ms: u64, views: &[ViewRecord]) {
        state.begin_frame();
        state.begin_root_paint();
        state.finish_root_paint();
        state.finish_draw(
            Duration::from_millis(draw_ms),
            Duration::from_millis(1),
            views.iter().copied(),
        );
    }

    fn view(entity_id: u64, cause: Option<ViewRenderCause>) -> ViewRecord {
        ViewRecord {
            entity_id,
            type_name: if entity_id == 1 { "Root" } else { "Panel" },
            cause,
        }
    }

    #[test]
    fn duplicate_ids_are_omitted_and_reported() {
        let mut builder = TreeBuilder::default();
        assert!(builder.record(node("same", None)));
        assert!(!builder.record(node("same", None)));

        let state = SharedState::new();
        state.publish_nodes([node("same", None), node("same", None)], false);
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
        state.publish_nodes(
            [
                node("missing", Some("absent")),
                node("a", Some("b")),
                node("b", Some("a")),
            ],
            false,
        );

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
            state.publish_nodes([node("stable", None)], false);
        }
        assert_eq!(state.tree().generation, 1);
    }

    #[test]
    fn an_incomplete_frame_is_reported_in_the_next_tree() {
        let state = SharedState::new();
        state.publish_nodes([node("stable", None)], true);
        assert!(
            state
                .tree()
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == SemanticDiagnosticCode::InvalidNode)
        );
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

        state.publish_nodes([node("published", None)], false);
        state.semantic_frames.send_modify(|count| *count += 1);

        let tree = waiter
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.message)?;
        assert_eq!(tree.generation, 1);
        assert!(tree.nodes.contains_key("published"));
        Ok(())
    }

    #[tokio::test]
    async fn frame_wait_wakes_after_the_draw_completes() -> Result<(), String> {
        let state = SharedState::new();
        draw_frame(&state, 2, &[]);

        let waiter_state = state.clone();
        let waiter =
            tokio::spawn(
                async move { waiter_state.wait_for_frame(1, Duration::from_secs(1)).await },
            );
        tokio::task::yield_now().await;

        draw_frame(&state, 2, &[]);

        let observed_frame = waiter
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.message)?;
        assert_eq!(observed_frame.frame_count, 2);
        Ok(())
    }

    fn complete_frame(state: &SharedState) {
        draw_frame(state, 1, &[]);
    }

    /// Root paint is not presentation: a painted frame alone must not release a
    /// presented-frame wait, only the presentation mark that follows it does.
    #[tokio::test]
    async fn presented_frame_wait_waits_for_the_presentation_mark() -> Result<(), String> {
        let state = SharedState::new();
        complete_frame(&state);
        state.mark_frames_presented();
        assert_eq!(state.presented_frame_count(), 1);

        complete_frame(&state);
        let wait = state.wait_for_presented_frame(1, Duration::from_secs(1));
        tokio::pin!(wait);
        tokio::select! {
            biased;
            result = &mut wait => {
                return Err(format!(
                    "presented wait completed before the frame was presented: {:?}",
                    result.map_err(|error| error.message)
                ));
            }
            () = tokio::task::yield_now() => {}
        }

        state.mark_frames_presented();
        let observed = wait.await.map_err(|error| error.message)?;
        assert_eq!(observed.frame_count, 2);
        assert_eq!(state.presented_frame_count(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn presentation_mark_without_a_new_frame_does_not_release_a_wait() {
        let state = SharedState::new();
        complete_frame(&state);
        state.mark_frames_presented();
        state.mark_frames_presented();

        let result = state
            .wait_for_presented_frame(1, Duration::from_millis(20))
            .await
            .map_err(|error| error.code);
        assert_eq!(result, Err(ErrorCode::Timeout));
        assert_eq!(state.presented_frame_count(), 1);
    }

    #[tokio::test]
    async fn presented_frame_wait_returns_at_once_for_an_already_presented_frame()
    -> Result<(), String> {
        let state = SharedState::new();
        complete_frame(&state);
        complete_frame(&state);
        state.mark_frames_presented();

        let observed = state
            .wait_for_presented_frame(1, Duration::from_millis(20))
            .await
            .map_err(|error| error.message)?;
        assert_eq!(observed.frame_count, 2);
        Ok(())
    }

    #[tokio::test]
    async fn frame_wait_never_returns_a_started_but_incomplete_frame() -> Result<(), String> {
        let state = SharedState::new();
        draw_frame(&state, 2, &[]);

        state.begin_frame();
        state.begin_root_paint();
        state.finish_root_paint();
        assert_eq!(state.frame_stats().frame_count, 1);

        let wait = state.wait_for_frame(1, Duration::from_secs(1));
        tokio::pin!(wait);
        tokio::select! {
            biased;
            result = &mut wait => {
                return Err(format!(
                    "frame wait completed before the draw finished: {:?}",
                    result.map_err(|error| error.message)
                ));
            }
            () = tokio::task::yield_now() => {}
        }

        state.finish_draw(Duration::from_millis(3), Duration::ZERO, []);
        let observed = wait.await.map_err(|error| error.message)?;
        assert_eq!(observed.frame_count, 2);
        assert_eq!(state.frame_stats(), observed);
        Ok(())
    }

    #[test]
    fn a_mark_isolates_the_frames_drawn_after_it() {
        let state = SharedState::new();
        draw_frame(&state, 40, &[view(1, Some(ViewRenderCause::Refresh))]);
        draw_frame(&state, 40, &[view(1, Some(ViewRenderCause::Refresh))]);

        let marked = state.mark_frames();
        assert_eq!(marked.mark_frame_count, 2);
        assert!(
            marked.draw_average_ms.abs() < f64::EPSILON,
            "a mark resets the rolling averages"
        );

        for draw_ms in [2, 4, 6, 8] {
            draw_frame(
                &state,
                draw_ms,
                &[
                    view(1, Some(ViewRenderCause::Uncached)),
                    view(2, Some(ViewRenderCause::Notified)),
                    view(3, None),
                ],
            );
        }

        let averages = state.frame_stats();
        assert!((averages.draw_average_ms - 5.0).abs() < 1e-9);
        assert!((averages.draw_max_ms - 8.0).abs() < 1e-9);

        let report = state.frame_report(None, 2);
        assert_eq!(report.after_frame_count, 2);
        assert_eq!(report.latest_frame_count, 6);
        assert!(!report.truncated);
        assert_eq!(report.summary.frames, 4);
        assert!((report.summary.draw_ms.p50 - 4.0).abs() < 1e-9);
        assert!((report.summary.draw_ms.p95 - 8.0).abs() < 1e-9);
        assert!((report.summary.app_draw_ms.max - 7.0).abs() < 1e-9);
        assert_eq!(
            report
                .frames
                .iter()
                .map(|frame| frame.frame_count)
                .collect::<Vec<_>>(),
            [5, 6],
            "the frame limit keeps the most recent samples"
        );
        assert_eq!(report.frames[0].views_rendered, 2);
        assert_eq!(report.frames[0].views_reused, 1);

        let reused = report
            .views
            .iter()
            .find(|activity| activity.entity_id == 3)
            .map(|activity| (activity.rendered, activity.reused));
        assert_eq!(reused, Some((0, 4)));
        let notified = report
            .views
            .iter()
            .find(|activity| activity.entity_id == 2)
            .and_then(|activity| activity.causes.get(&ViewRenderCause::Notified).copied());
        assert_eq!(notified, Some(4));
        assert!(
            report
                .views
                .iter()
                .all(|activity| !activity.causes.contains_key(&ViewRenderCause::Refresh)),
            "frames before the mark must not leak into the report"
        );
        assert_eq!(report.last_frame_views.len(), 3);

        let explicit = state.frame_report(Some(0), 10);
        assert_eq!(explicit.summary.frames, 6);
    }

    #[test]
    fn a_report_says_when_frames_after_its_mark_were_evicted() {
        let state = SharedState::new();
        for _ in 0..(super::MAX_FRAME_SAMPLES + 3) {
            draw_frame(&state, 1, &[]);
        }
        let report = state.frame_report(Some(0), 1);
        assert!(report.truncated);
        assert_eq!(
            usize::try_from(report.summary.frames).unwrap_or_default(),
            super::MAX_FRAME_SAMPLES
        );

        let recent = state.frame_report(Some(4), 1);
        assert!(!recent.truncated);
    }
}
