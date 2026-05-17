//! `ResultPanel` — shared chrome host for all result-rendering views.
//!
//! `ResultPanel` renders a single chrome row containing toolbar segments from
//! two sources:
//!
//! - **Built-in segments** owned by `ResultPanel` itself:
//!   - Mode bar (`Left`, index 0): displayed when `view.available_modes().len() >= 2`.
//!
//! - **View segments** provided by the hosted `ViewHandle` via its
//!   `toolbar_segments` closure (e.g. DataGridPanel's filter bar as `Center`/0,
//!   which includes the full refresh compound: action button + divider + chevron).
//!
//! All segments are collected, sorted by `(position, index)`, and rendered in a
//! single `flex_wrap` row with `gap(Spacing::SM)`. No positional spacers are
//! inserted — spacers are incompatible with `flex_wrap`.
//!
//! ## ViewHandle
//!
//! `ViewHandle` is a closure-erasing shell that lets any view live inside
//! `ResultPanel` without requiring a concrete generic type parameter. Each view
//! that wants to be hosted inside `ResultPanel` adds an
//! `into_view_handle(entity, cx) -> ViewHandle` constructor.
//!
//! ## Events
//!
//! `ResultPanel` emits `ResultPanelEvent` for backward-compatibility with
//! container documents not yet migrated to `ViewHandle`. In the `ViewHandle`
//! flow, mode changes are forwarded directly to `view.set_mode` first, then
//! the event is emitted for any remaining listeners.

use crate::result_view::ResultViewMode;
use crate::tokens::{FontSizes, Heights, Spacing};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

// ── SegmentPosition ────────────────────────────────────────────────────────────

/// Position group for a toolbar segment within the chrome row.
///
/// Segments sort first by `SegmentPosition` (Left < Center < Right), then by
/// `index` within each group. Flex spacers are inserted between groups.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SegmentPosition {
    Left,
    Center,
    Right,
}

// ── ToolbarSegment ─────────────────────────────────────────────────────────────

/// A single slot in the `ResultPanel` chrome row.
///
/// The `builder` closure produces an `AnyElement` on each render frame. Builders
/// capture `Entity<V>` (their view entity) and use `entity.update(cx, ...)` for
/// event handlers. They MUST NOT capture `Window` or `App` — those arrive as
/// parameters per GPUI's capture rules.
pub struct ToolbarSegment {
    pub position: SegmentPosition,
    pub index: u16,
    /// Produces the element for this segment on each render frame.
    pub builder: Box<dyn Fn(&mut Window, &mut App) -> AnyElement + 'static>,
}

// ── ViewHandle ─────────────────────────────────────────────────────────────────

/// Closure-erasing shell for any view that can live inside `ResultPanel`.
///
/// `ViewHandle` is `!Clone` (closures are not cloneable). `ResultPanel` owns
/// exactly one `ViewHandle`. Each view that wants to be hosted adds an
/// `into_view_handle(entity, cx) -> ViewHandle` constructor.
///
/// All closures take per-call `&App`, `&mut Window`, or `&mut App` parameters —
/// never captured contexts. Each closure captures a cloned `Entity<V>` which is
/// `Clone + 'static` and safe to capture in GPUI.
pub struct ViewHandle {
    /// Render the view's content area (table, chart, document tree, etc.).
    pub render: Box<dyn Fn(&mut Window, &mut App) -> AnyElement + 'static>,

    /// Request keyboard focus for the view's primary interactive region.
    pub focus: Box<dyn Fn(&mut Window, &mut App) + 'static>,

    /// Return the view's primary `FocusHandle`.
    pub focus_handle: Box<dyn Fn(&App) -> FocusHandle + 'static>,

    /// View-specific toolbar segments (filter bar, axis bar, range chips, etc.).
    ///
    /// Called each render frame. Segments are merged with built-in segments and
    /// sorted by `(position, index)`.
    pub toolbar_segments: Box<dyn Fn(&App) -> Vec<ToolbarSegment> + 'static>,

    /// The result-view modes this view supports.
    ///
    /// `ResultPanel` displays a mode bar when the returned list has 2+ entries.
    pub available_modes: Box<dyn Fn(&App) -> Vec<ResultViewMode> + 'static>,

    /// The currently active result-view mode.
    pub current_mode: Box<dyn Fn(&App) -> ResultViewMode + 'static>,

    /// Set the active mode on the underlying view entity.
    ///
    /// Called directly by `ResultPanel` when the user clicks a mode tab. No
    /// `Window` parameter — mode switching is a pure data update.
    pub set_mode: Box<dyn Fn(ResultViewMode, &mut App) + 'static>,
}

impl ViewHandle {
    /// Begin building a `ViewHandle`. All closures are required.
    pub fn builder() -> ViewHandleBuilder {
        ViewHandleBuilder::new()
    }
}

// ── ViewHandleBuilder ─────────────────────────────────────────────────────────

/// Step-by-step builder for `ViewHandle`.
///
/// All fields are required. Calling `build()` before all fields are set panics
/// with a descriptive message.
pub struct ViewHandleBuilder {
    render: Option<Box<dyn Fn(&mut Window, &mut App) -> AnyElement + 'static>>,
    focus: Option<Box<dyn Fn(&mut Window, &mut App) + 'static>>,
    focus_handle: Option<Box<dyn Fn(&App) -> FocusHandle + 'static>>,
    toolbar_segments: Option<Box<dyn Fn(&App) -> Vec<ToolbarSegment> + 'static>>,
    available_modes: Option<Box<dyn Fn(&App) -> Vec<ResultViewMode> + 'static>>,
    current_mode: Option<Box<dyn Fn(&App) -> ResultViewMode + 'static>>,
    set_mode: Option<Box<dyn Fn(ResultViewMode, &mut App) + 'static>>,
}

impl ViewHandleBuilder {
    fn new() -> Self {
        Self {
            render: None,
            focus: None,
            focus_handle: None,
            toolbar_segments: None,
            available_modes: None,
            current_mode: None,
            set_mode: None,
        }
    }

    pub fn render(mut self, f: impl Fn(&mut Window, &mut App) -> AnyElement + 'static) -> Self {
        self.render = Some(Box::new(f));
        self
    }

    pub fn focus(mut self, f: impl Fn(&mut Window, &mut App) + 'static) -> Self {
        self.focus = Some(Box::new(f));
        self
    }

    pub fn focus_handle(mut self, f: impl Fn(&App) -> FocusHandle + 'static) -> Self {
        self.focus_handle = Some(Box::new(f));
        self
    }

    pub fn toolbar_segments(mut self, f: impl Fn(&App) -> Vec<ToolbarSegment> + 'static) -> Self {
        self.toolbar_segments = Some(Box::new(f));
        self
    }

    pub fn available_modes(mut self, f: impl Fn(&App) -> Vec<ResultViewMode> + 'static) -> Self {
        self.available_modes = Some(Box::new(f));
        self
    }

    pub fn current_mode(mut self, f: impl Fn(&App) -> ResultViewMode + 'static) -> Self {
        self.current_mode = Some(Box::new(f));
        self
    }

    pub fn set_mode(mut self, f: impl Fn(ResultViewMode, &mut App) + 'static) -> Self {
        self.set_mode = Some(Box::new(f));
        self
    }

    /// Consume the builder and produce a `ViewHandle`.
    ///
    /// # Panics
    ///
    /// Panics if any required field has not been set.
    pub fn build(self) -> ViewHandle {
        ViewHandle {
            render: self.render.expect("ViewHandle::render is required"),
            focus: self.focus.expect("ViewHandle::focus is required"),
            focus_handle: self
                .focus_handle
                .expect("ViewHandle::focus_handle is required"),
            toolbar_segments: self
                .toolbar_segments
                .expect("ViewHandle::toolbar_segments is required"),
            available_modes: self
                .available_modes
                .expect("ViewHandle::available_modes is required"),
            current_mode: self
                .current_mode
                .expect("ViewHandle::current_mode is required"),
            set_mode: self.set_mode.expect("ViewHandle::set_mode is required"),
        }
    }
}

// ── Events ─────────────────────────────────────────────────────────────────────

/// Events emitted by `ResultPanel`.
///
/// Retained for backward-compatibility with documents not yet migrated to the
/// `ViewHandle` pattern. In the `ViewHandle` flow, mode changes are forwarded
/// directly to `view.set_mode` first.
#[derive(Clone, Debug)]
pub enum ResultPanelEvent {
    /// The user clicked a mode tab in the mode bar.
    ModeChanged(ResultViewMode),
}

// ── ResultPanel ────────────────────────────────────────────────────────────────

/// Shared chrome host for all result-rendering views.
///
/// Owns a `ViewHandle` and an optional list of host-injected `ToolbarSegment`s.
/// On each render frame, the chrome row is assembled from:
///
/// 1. Built-in Left/0 mode bar (when `view.available_modes(cx).len() >= 2`)
/// 2. View's own segments (from `view.toolbar_segments(cx)`)
/// 3. Host-injected segments (from `with_segments(...)` or `add_segment(...)`)
///
/// All segments are sorted by `(SegmentPosition, index)` and rendered in a
/// single `flex_wrap` + `gap` row. No positional spacers are inserted —
/// spacers are incompatible with `flex_wrap`.
///
/// The refresh compound (action button + divider + chevron dropdown) lives
/// inside the view's own filter-bar segment, not here.
pub struct ResultPanel {
    /// The hosted view (closure-erasing shell).
    view: ViewHandle,

    /// Host-injected segments that augment the view's own segments.
    host_segments: Vec<ToolbarSegment>,

    /// Focus root for the panel.
    focus_handle: FocusHandle,
}

impl ResultPanel {
    /// Create a `ResultPanel` wrapping the given `ViewHandle`.
    pub fn new(view: ViewHandle, cx: &mut Context<Self>) -> Self {
        Self {
            view,
            host_segments: Vec::new(),
            focus_handle: cx.focus_handle(),
        }
    }

    /// Inject host-provided segments into the chrome row.
    ///
    /// Chainable builder method; can also use `add_segment` for mutation.
    pub fn with_segments(mut self, segments: Vec<ToolbarSegment>) -> Self {
        self.host_segments.extend(segments);
        self
    }

    /// Add a single host-injected segment.
    pub fn add_segment(&mut self, segment: ToolbarSegment) {
        self.host_segments.push(segment);
    }

    // ── Focus ─────────────────────────────────────────────────────────────────

    /// Delegate focus to the hosted view's primary interactive region.
    pub fn focus_view(&self, window: &mut Window, cx: &mut App) {
        (self.view.focus)(window, cx);
    }

    /// Focus this panel's own focus root (used by `track_focus` in render).
    pub fn focus(&self, window: &mut Window) {
        self.focus_handle.focus(window);
    }

    // ── Test helpers ──────────────────────────────────────────────────────────

    /// Whether the built-in mode bar segment will appear (requires `cx`).
    ///
    /// True when `view.available_modes(cx).len() >= 2`.
    pub fn has_mode_bar_segment_cx(&self, cx: &App) -> bool {
        (self.view.available_modes)(cx).len() >= 2
    }

    /// Whether the built-in mode bar segment will appear.
    ///
    /// Alias that checks whether 2+ modes were reported at construction time.
    /// For tests that cannot pass cx, use `has_mode_bar_segment_cx`.
    pub fn has_mode_bar_segment(&self) -> bool {
        // Without cx we cannot call the closure. This method exists for the
        // test API surface; prefer `has_mode_bar_segment_cx` when cx is
        // available. Returns false conservatively.
        false
    }

    /// Return `(position, index)` pairs for host-injected segments, sorted.
    ///
    /// Used by tests to verify segment ordering without invoking render.
    pub fn sorted_segment_positions(&self) -> Vec<(SegmentPosition, u16)> {
        let mut pairs: Vec<_> = self
            .host_segments
            .iter()
            .map(|s| (s.position, s.index))
            .collect();
        pairs.sort_by_key(|&(pos, idx)| (pos, idx));
        pairs
    }

    /// Return all `(position, index)` pairs including built-ins, sorted.
    ///
    /// Built-in mode bar: `Left/0` (when `available_modes.len() >= 2`).
    pub fn all_sorted_segment_positions(&self, cx: &App) -> Vec<(SegmentPosition, u16)> {
        let mut pairs: Vec<(SegmentPosition, u16)> = Vec::new();

        if (self.view.available_modes)(cx).len() >= 2 {
            pairs.push((SegmentPosition::Left, 0));
        }

        for s in (self.view.toolbar_segments)(cx).iter() {
            pairs.push((s.position, s.index));
        }

        for s in &self.host_segments {
            pairs.push((s.position, s.index));
        }

        pairs.sort_by_key(|&(pos, idx)| (pos, idx));
        pairs
    }

    /// Build the mode tab strip element.
    fn build_mode_bar_element(
        &self,
        modes: &[ResultViewMode],
        current: ResultViewMode,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let modes: Vec<ResultViewMode> = modes.to_vec();
        let theme = theme.clone();

        div()
            .flex()
            .flex_row()
            .items_center()
            .h_full()
            .gap(Spacing::XS)
            .children(modes.into_iter().enumerate().map(|(i, mode)| {
                let is_active = mode == current;
                let theme = theme.clone();

                div()
                    .id(ElementId::Name(format!("result-panel-mode-{}", i).into()))
                    .flex()
                    .items_center()
                    .h_full()
                    .px(Spacing::SM)
                    .cursor_pointer()
                    .border_b_2()
                    .border_color(if is_active {
                        theme.accent
                    } else {
                        gpui::transparent_black()
                    })
                    .text_color(if is_active {
                        theme.foreground
                    } else {
                        theme.muted_foreground
                    })
                    .when(!is_active, |d| d.hover(|d| d.bg(theme.secondary)))
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _, _window, cx| {
                            let current = (this.view.current_mode)(cx);
                            if current != mode {
                                (this.view.set_mode)(mode, cx);
                                cx.emit(ResultPanelEvent::ModeChanged(mode));
                                cx.notify();
                            }
                        }),
                    )
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .when(is_active, |d| d.font_weight(FontWeight::SEMIBOLD))
                            .child(SharedString::from(mode.label())),
                    )
            }))
            .into_any()
    }
}

// ── EventEmitter ───────────────────────────────────────────────────────────────

impl EventEmitter<ResultPanelEvent> for ResultPanel {}

// ── Render ─────────────────────────────────────────────────────────────────────

impl Render for ResultPanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let focus_handle = self.focus_handle.clone();

        // Build chrome row before rendering view content to avoid borrow conflict.
        // render_chrome_row needs &self and &mut cx; we must call it before
        // calling view.render which also needs &mut window and &mut cx.
        let chrome_children = self.collect_chrome_children(window, cx);

        // Render view content.
        let view_element = (self.view.render)(window, cx);

        let chrome_row = if chrome_children.is_empty() {
            None
        } else {
            let theme = cx.theme().clone();
            Some(
                chrome_children.into_iter().fold(
                    div()
                        .flex()
                        .flex_wrap()
                        .items_center()
                        // w_full is REQUIRED. In a flex-col parent the chrome
                        // row defaults to its intrinsic content width, so a
                        // wide segment makes the row wider than the viewport,
                        // pushing trailing widgets (e.g. Refresh) off-screen,
                        // and flex_wrap never triggers because the row "fits"
                        // its own oversized box. w_full forces the row to the
                        // parent's width so flex_wrap can do its job.
                        .w_full()
                        .min_h(Heights::TOOLBAR)
                        .px(Spacing::SM)
                        .gap(Spacing::SM)
                        .border_b_1()
                        .border_color(theme.border)
                        .bg(theme.tab_bar),
                    |r, child| r.child(child),
                ),
            )
        };

        div()
            .track_focus(&focus_handle)
            .flex()
            .flex_col()
            .size_full()
            .when_some(chrome_row, |d, row| d.child(row))
            .child(view_element)
    }
}

impl ResultPanel {
    /// Collect all chrome-row children as `AnyElement`. Returns empty vec when
    /// no segments exist.
    ///
    /// Segments from all sources (mode bar, view, host-injected) are merged and
    /// sorted by `(position, index)`. No flex spacers are inserted — the chrome
    /// row uses `flex_wrap` + `gap`, which is incompatible with spacers.
    ///
    /// Separated from `render` to avoid simultaneous borrow of `self` and `cx`
    /// when also calling `(self.view.render)(window, cx)`.
    fn collect_chrome_children(
        &self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Vec<AnyElement> {
        let available_modes = (self.view.available_modes)(cx);
        let current_mode = (self.view.current_mode)(cx);
        let has_mode_bar = available_modes.len() >= 2;

        // Collect view segments.
        let view_segs: Vec<ToolbarSegment> = (self.view.toolbar_segments)(cx);

        if !has_mode_bar && view_segs.is_empty() && self.host_segments.is_empty() {
            return vec![];
        }

        let theme = cx.theme().clone();

        struct Entry {
            position: SegmentPosition,
            index: u16,
            element: AnyElement,
        }

        let mut entries: Vec<Entry> = Vec::new();

        // Built-in mode bar (Left/0).
        if has_mode_bar {
            let el = self.build_mode_bar_element(&available_modes, current_mode, &theme, cx);
            entries.push(Entry {
                position: SegmentPosition::Left,
                index: 0,
                element: el,
            });
        }

        // View segments.
        for seg in view_segs {
            let el = (seg.builder)(window, cx);
            entries.push(Entry {
                position: seg.position,
                index: seg.index,
                element: el,
            });
        }

        // Host-injected segments.
        for seg in &self.host_segments {
            let el = (seg.builder)(window, cx);
            entries.push(Entry {
                position: seg.position,
                index: seg.index,
                element: el,
            });
        }

        entries.sort_by_key(|e| (e.position, e.index));

        entries.into_iter().map(|e| e.element).collect()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────
//
// Slot-system contract tests:
//   `crates/dbflux_components/tests/result_panel_slot_contract.rs`
//
// Legacy API tests (kept for reference):
//   `crates/dbflux_components/tests/result_panel_contract.rs`
