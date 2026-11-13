//! Hardened, cross-platform MCP automation for GPUI applications.
//!
//! Install [`BridgeHandle`] for a window and retain it for that window's
//! lifetime. GPUI emits semantics automatically from stable element IDs,
//! registered interaction behavior, and its standard `Role` and `aria_*` APIs.
//! Control stays on owner-restricted native local IPC and authenticated
//! requests are dispatched through GPUI's foreground executor.

#[cfg(any(
    all(feature = "zed", feature = "gpui-pre"),
    all(feature = "zed", feature = "gpui-ce"),
    all(feature = "gpui-pre", feature = "gpui-ce"),
))]
compile_error!(
    "select one GPUI backend: use default-features = false with features = [\"gpui-pre\"] \
     for gpui-kit, or features = [\"gpui-ce\"] for gpui-ce"
);
#[cfg(not(any(feature = "zed", feature = "gpui-pre", feature = "gpui-ce")))]
compile_error!("select a GPUI backend: enable `zed` (the default), `gpui-pre` or `gpui-ce`");

#[cfg(feature = "gpui-ce")]
extern crate gpui_ce as gpui;
#[cfg(feature = "gpui-pre")]
extern crate gpui_pre as gpui;

mod annotations;
mod input;
mod messages;
mod native_window;
mod observer;
mod registry;
mod service;

use std::sync::Arc;

pub use gpui_mcp_protocol::MouseButton;
pub use gpui_mcp_protocol::{
    Annotation, AnnotationSource, AnnotationSpec, AnnotationStyle, AnnotationTarget,
    DEFAULT_ANNOTATION_COLOR, MAX_ANNOTATIONS, MAX_MESSAGE_PAGE, MAX_MESSAGE_TEXT_BYTES,
    MAX_RETAINED_MESSAGES, MAX_UNREAD_MESSAGES, Message, MessagePage, MessageSender, NewMessage,
};
pub use gpui_mcp_protocol::{
    AppId, ApplicationCommandDescriptor, ApplicationCommandResult, BridgeError, ContextResource,
    ContextResourceDescriptor, Distribution, ErrorCode, FrameReport, FrameSample, FrameStats,
    FrameSummary, InstanceId, LiveDocument, LiveDocumentDiagnostic, LiveDocumentPreview,
    LiveDocumentSource, LogEntry, MAX_FRAME_SAMPLES, MAX_FRAME_VIEWS, MAX_LABEL_BYTES,
    MAX_LIVE_DOCUMENT_DIAGNOSTICS, MAX_LIVE_DOCUMENT_SOURCE_BYTES, MAX_TEXT_BYTES, NativeWindowId,
    NodeAction, NodeState, Point, ProcessId, Rect, RequestId, Role, TextInfo, TextRange, UiNode,
    UiTree, ValueInfo, ViewActivity, ViewDraw, ViewOutcome, ViewRenderCause,
};
pub use service::{
    AnnotationEvent, ApplicationCommandRequest, ApplicationCommandResponse, BridgeConfig,
    BridgeConfigError, BridgeHandle, ContextResourceRequest, ContextResourceResponse, HostError,
    LiveDocumentRequest, LiveDocumentResponse, StartError,
};

use observer::BridgeObserver;
use registry::SharedState;

/// Return the operating system identifier used by window capture APIs.
///
/// Wayland and other platforms without a stable system window identifier
/// return `None`.
#[must_use]
pub fn native_window_id(window: &gpui::Window) -> Option<NativeWindowId> {
    native_window::id(window).and_then(NativeWindowId::new)
}

/// Cloneable application-side handle for semantic snapshots and diagnostic logs.
#[derive(Clone)]
pub struct Automation {
    pub(crate) state: Arc<SharedState>,
    observer: Arc<BridgeObserver>,
}

impl Automation {
    /// Create isolated in-process automation without starting an MCP bridge.
    ///
    /// This is intended for offline application modes and embedded previews that
    /// still want one local semantic tree. No endpoint, listener, descriptor, or
    /// background thread is created.
    #[must_use]
    pub fn isolated() -> Self {
        Self::new(SharedState::new())
    }

    fn new(state: Arc<SharedState>) -> Self {
        let observer = BridgeObserver::new(&state);
        Self { state, observer }
    }

    /// Attach automatic semantic observation to a window.
    ///
    /// Calling this more than once for the same automation and window is a no-op.
    pub fn attach(&self, window: &mut gpui::Window) {
        window.observe_frames(&self.observer);
        window.refresh();
    }

    /// Create isolated in-process automation without IPC for GPUI runtime tests.
    ///
    /// This constructor is available only with the `test-support` feature and
    /// must not be used as a substitute for [`BridgeHandle`] in an application.
    #[cfg(feature = "test-support")]
    #[must_use]
    pub fn for_test() -> Self {
        Self::isolated()
    }

    /// Return the most recently completed semantic frame.
    ///
    /// A frame is converted into a tree when it is first read rather than while it is drawn,
    /// so the first read after a frame pays for that conversion.
    #[must_use]
    pub fn snapshot(&self) -> UiTree {
        self.state.tree()
    }

    /// Return the generation of the most recently completed semantic frame without cloning it.
    ///
    /// Consumers that maintain a small derived view of the semantic tree can use this as an
    /// invalidation guard and call [`Self::snapshot`] only after the generation changes. The
    /// first call after a frame converts that frame, as [`Self::snapshot`] would.
    #[must_use]
    pub fn semantic_generation(&self) -> u64 {
        self.state.tree_generation()
    }

    /// Return timing statistics for the frames completed since the last mark.
    #[must_use]
    pub fn frame_stats(&self) -> FrameStats {
        self.state.frame_stats()
    }

    /// Start a measurement window at the last completed frame.
    ///
    /// [`Self::frame_stats`] and [`Self::frame_report`] then cover only later frames. Call this
    /// between draws, for example from an event handler, so no frame straddles the mark.
    pub fn mark_frames(&self) {
        self.state.mark_frames();
    }

    /// Return per-frame samples, percentiles, and view-cache activity for retained frames
    /// completed after `after_frame_count`, or after the last mark when it is `None`.
    ///
    /// At most `frame_limit` per-frame samples are returned, the most recent last; the summary
    /// and view activity cover every retained frame.
    #[must_use]
    pub fn frame_report(&self, after_frame_count: Option<u64>, frame_limit: usize) -> FrameReport {
        self.state.frame_report(after_frame_count, frame_limit)
    }

    /// Return the count of the most recently completed root-paint frame.
    ///
    /// This is available only to deterministic runtime tests. A frame is not
    /// counted until the observed window has finished painting.
    #[cfg(feature = "test-support")]
    #[must_use]
    pub fn completed_frames(&self) -> u64 {
        self.state.frame_stats().frame_count
    }

    /// Retain a bounded, sanitized diagnostic log entry for MCP inspection.
    ///
    /// Do not pass secrets. Newlines are replaced and messages are capped at 4 KiB.
    pub fn log(&self, level: &str, message: &str) {
        self.state.add_log(level, message);
    }

    /// Add or replace one annotation by id, as the application.
    ///
    /// A node target is resolved again on every frame, so the annotation
    /// follows the element through layout changes, scrolling and zoom, and is
    /// hidden while the element is absent. The window is asked for a frame so
    /// the change shows without other invalidation.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorCode::InvalidRequest`] for a spec outside the bounds, or
    /// [`ErrorCode::Busy`] when 128 annotations already exist.
    ///
    /// [`ErrorCode::InvalidRequest`]: gpui_mcp_protocol::ErrorCode::InvalidRequest
    /// [`ErrorCode::Busy`]: gpui_mcp_protocol::ErrorCode::Busy
    pub fn annotate(
        &self,
        spec: AnnotationSpec,
        window: &mut gpui::Window,
    ) -> Result<Annotation, BridgeError> {
        let mut applied = self.set_annotations(vec![spec], None, window)?;
        applied.pop().ok_or_else(|| {
            BridgeError::new(
                gpui_mcp_protocol::ErrorCode::Internal,
                "annotation was not applied",
            )
        })
    }

    /// Add or replace several annotations at once, first removing every
    /// annotation in `replace_group` when it is given. Either every spec
    /// applies or none does.
    ///
    /// # Errors
    ///
    /// See [`Self::annotate`].
    pub fn set_annotations(
        &self,
        specs: Vec<AnnotationSpec>,
        replace_group: Option<&str>,
        window: &mut gpui::Window,
    ) -> Result<Vec<Annotation>, BridgeError> {
        let applied = self
            .state
            .upsert_annotations(specs, replace_group, AnnotationSource::App)?;
        window.request_frame();
        Ok(applied)
    }

    /// Remove annotations by id, and return how many were removed.
    pub fn remove_annotations(&self, ids: &[impl AsRef<str>], window: &mut gpui::Window) -> usize {
        let removed = self.state.remove_annotations(ids, AnnotationSource::App);
        window.request_frame();
        removed
    }

    /// Remove every annotation, or every one in `group`, and return how many were removed.
    pub fn clear_annotations(&self, group: Option<&str>, window: &mut gpui::Window) -> usize {
        let removed = self.state.clear_annotations(group, AnnotationSource::App);
        window.request_frame();
        removed
    }

    /// Every current annotation in draw order, each with the bounds it was
    /// drawn at in the most recent frame.
    #[must_use]
    pub fn annotations(&self) -> Vec<Annotation> {
        self.state.annotations()
    }

    /// A number that increases whenever annotations are added, changed or removed.
    #[must_use]
    pub fn annotation_revision(&self) -> u64 {
        self.state.annotation_revision()
    }

    /// Choose whether the bridge draws annotations. They are still resolved
    /// every frame, so an application that draws them itself can read their
    /// bounds from [`Self::annotations`].
    pub fn paint_annotations(&self, paint: bool, window: &mut gpui::Window) {
        self.state.set_paint_annotations(paint);
        window.request_frame();
    }

    /// Post a message for the connected agent.
    ///
    /// The agent reads it with the `read_messages` or `wait_for_messages` tool,
    /// or through the `gpui://messages` resource. There is no model in the
    /// bridge: the message waits in a bounded log until the agent reads it.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorCode::InvalidRequest`] for a message outside the bounds,
    /// or [`ErrorCode::Busy`] while the agent has 64 of the app's messages unread.
    ///
    /// [`ErrorCode::InvalidRequest`]: gpui_mcp_protocol::ErrorCode::InvalidRequest
    /// [`ErrorCode::Busy`]: gpui_mcp_protocol::ErrorCode::Busy
    pub fn post_message(&self, message: NewMessage) -> Result<Message, BridgeError> {
        self.state.post_message(MessageSender::App, message)
    }

    /// Read retained messages after id `after`, oldest first, optionally only
    /// from one side. Messages from the agent in the page count as read by the
    /// application.
    #[must_use]
    pub fn read_messages(
        &self,
        after: u64,
        from: Option<MessageSender>,
        limit: usize,
    ) -> MessagePage {
        self.state
            .read_messages(after, from, limit, Some(MessageSender::App))
    }

    /// Most recent log entries in chronological order, filtered to `min_level`
    /// ("debug" < "info" < "warn" < "error") when given, capped at `limit`.
    #[must_use]
    pub fn logs(&self, limit: u16, min_level: Option<&str>) -> Vec<LogEntry> {
        self.state.logs(limit, min_level)
    }
}
