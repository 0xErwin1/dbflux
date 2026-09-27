//! Workspace-level inspector rail.
//!
//! `WorkspaceInspector` is a persistent right-edge panel that renders arbitrary
//! `AnyView` content supplied by the event chain originating from
//! `DataGridPanel::open_row_inspector`.  The workspace owns this entity for
//! its entire lifetime; per-tab visibility is driven by `OpenInspector` /
//! `CloseInspector` events the active document emits on `set_active_tab`,
//! so the rail follows the active tab instead of bleeding stale content
//! across tab switches.
//!
//! # Resize
//!
//! The rail is its own island, `IslandMetrics::GAP` of desk to the right of
//! the document island. `width` is the island's width. The grip (6 px) over
//! the island's left edge starts the drag on `mouse_down`.  Move and up
//! events are captured by a workspace-root drag mask (an absolute overlay
//! rendered only while `is_resizing == true`) so the cursor is tracked
//! anywhere on screen.  When the drag ends the inspector emits
//! `ResizeCommitted(final_width)` so the workspace can persist the width.
//!
//! # ESC / close
//!
//! The × button calls `close()` directly.  ESC is handled in
//! `workspace/dispatch.rs` as a fallback after the active document declines
//! Cancel: it calls `close()` and returns `true`.

use dbflux_components::composites::Island;
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::{ChromeColors, InspectorMetrics, IslandMetrics};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const INSPECTOR_MIN_WIDTH: Pixels = px(240.0);
pub const INSPECTOR_MAX_WIDTH: Pixels = px(1280.0);
pub const INSPECTOR_DEFAULT_WIDTH: Pixels = InspectorMetrics::WIDTH;
pub const INSPECTOR_GRIP_WIDTH: Pixels = px(6.0);

// ---------------------------------------------------------------------------
// Entity
// ---------------------------------------------------------------------------

/// Workspace-level inspector rail.
pub struct WorkspaceInspector {
    content: Option<AnyView>,
    title: SharedString,
    /// The content draws its own title bar, so the rail shows none.
    content_has_header: bool,
    /// Width the current content needs at least; the rail widens to it
    /// without changing the width the user chose for other content.
    content_min_width: Option<Pixels>,
    width: Pixels,
    is_open: bool,
    is_resizing: bool,
    resize_start_x: Option<Pixels>,
    resize_start_width: Option<Pixels>,
    focus_handle: FocusHandle,
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub enum WorkspaceInspectorEvent {
    /// Drag finished — caller persists the new width.
    ResizeCommitted(Pixels),
    /// User clicked close (×) or dispatched ESC fallback.
    Closed,
}

impl EventEmitter<WorkspaceInspectorEvent> for WorkspaceInspector {}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl WorkspaceInspector {
    pub fn new(initial_width: Pixels, cx: &mut Context<Self>) -> Self {
        let width = initial_width.clamp(INSPECTOR_MIN_WIDTH, INSPECTOR_MAX_WIDTH);
        Self {
            content: None,
            title: SharedString::default(),
            content_has_header: false,
            content_min_width: None,
            width,
            is_open: false,
            is_resizing: false,
            resize_start_x: None,
            resize_start_width: None,
            focus_handle: cx.focus_handle(),
        }
    }

    pub fn is_open(&self) -> bool {
        self.is_open
    }

    pub fn is_resizing(&self) -> bool {
        self.is_resizing
    }

    pub fn width(&self) -> Pixels {
        self.width
    }

    /// Width the rail draws at: the chosen width, widened to what the current
    /// content needs.
    pub fn rail_width(&self) -> Pixels {
        match self.content_min_width {
            Some(minimum) => self.width.max(minimum),
            None => self.width,
        }
    }

    fn resize_min_width(&self) -> Pixels {
        match self.content_min_width {
            Some(minimum) => INSPECTOR_MIN_WIDTH.max(minimum),
            None => INSPECTOR_MIN_WIDTH,
        }
    }

    pub fn title(&self) -> &SharedString {
        &self.title
    }

    /// Open / replace the inspector content. Reuses the rail if already open.
    ///
    /// `content_has_header` is set for content that draws its own title bar
    /// (the row inspector); the rail then shows only the content. The new
    /// content needs no minimum width until [`Self::set_content_min_width`]
    /// says otherwise.
    pub fn open_with(
        &mut self,
        content: AnyView,
        title: SharedString,
        content_has_header: bool,
        cx: &mut Context<Self>,
    ) {
        self.content = Some(content);
        self.title = title;
        self.content_has_header = content_has_header;
        self.content_min_width = None;
        self.is_open = true;
        cx.notify();
    }

    /// Widens the rail to at least `min_width` while the current content
    /// shows; `None` goes back to the chosen width.
    pub fn set_content_min_width(&mut self, min_width: Option<Pixels>, cx: &mut Context<Self>) {
        if self.content_min_width != min_width {
            self.content_min_width = min_width;
            cx.notify();
        }
    }

    /// Hide the rail without forgetting its content or emitting `Closed`.
    ///
    /// Used by per-tab visibility: when the user switches to a tab that has
    /// no inspector, the rail collapses but the previously-active tab's
    /// `inspector_row` state is left intact so the rail reappears on
    /// switch-back. Explicit user dismissal goes through `close` instead.
    pub fn hide(&mut self, cx: &mut Context<Self>) {
        if !self.is_open {
            return;
        }
        self.is_open = false;
        self.is_resizing = false;
        self.resize_start_x = None;
        self.resize_start_width = None;
        cx.notify();
    }

    /// Close the rail and emit `Closed`.
    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.is_open = false;
        self.content = None;
        self.is_resizing = false;
        self.resize_start_x = None;
        self.resize_start_width = None;
        cx.emit(WorkspaceInspectorEvent::Closed);
        cx.notify();
    }

    // -- Resize handlers driven by the workspace-level drag mask --

    /// Called on mouse_down on the grip; starts the resize gesture.
    pub(crate) fn begin_resize(&mut self, event: &MouseDownEvent, cx: &mut Context<Self>) {
        self.is_resizing = true;
        self.resize_start_x = Some(event.position.x);
        self.resize_start_width = Some(self.rail_width());
        cx.notify();
    }

    /// Simulate a drag-start at a given x position.
    ///
    /// Equivalent to the user pressing the left mouse button on the grip at
    /// `position_x`. Provided for testing without constructing `MouseDownEvent`.
    pub fn fake_begin_resize_at(&mut self, position_x: Pixels, cx: &mut Context<Self>) {
        self.is_resizing = true;
        self.resize_start_x = Some(position_x);
        self.resize_start_width = Some(self.rail_width());
        cx.notify();
    }

    /// Called on mouse_move via the workspace drag mask.
    pub fn update_resize(&mut self, position_x: Pixels, cx: &mut Context<Self>) {
        if !self.is_resizing {
            return;
        }
        let Some(start_x) = self.resize_start_x else {
            return;
        };
        let Some(start_width) = self.resize_start_width else {
            return;
        };
        // Drag-left grows the rail (rail lives on the right edge).
        let delta = position_x - start_x;
        let new_width = (start_width - delta).clamp(self.resize_min_width(), INSPECTOR_MAX_WIDTH);
        self.width = new_width;
        cx.notify();
    }

    /// Called on mouse_up via the workspace drag mask; commits the resize.
    pub fn finish_resize(&mut self, cx: &mut Context<Self>) {
        if !self.is_resizing {
            return;
        }
        let final_width = self.width;
        self.is_resizing = false;
        self.resize_start_x = None;
        self.resize_start_width = None;
        cx.emit(WorkspaceInspectorEvent::ResizeCommitted(final_width));
        cx.notify();
    }
}

impl Focusable for WorkspaceInspector {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

impl Render for WorkspaceInspector {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let is_resizing = self.is_resizing;
        let title = self.title.clone();
        let content = self.content.clone();
        let close_entity = cx.entity().clone();
        let rail_width = self.rail_width();

        let header = (!self.content_has_header).then(|| {
            div()
                .flex()
                .flex_shrink_0()
                .items_center()
                .gap(InspectorMetrics::HEADER_GAP)
                .h(InspectorMetrics::HEADER_HEIGHT)
                .pl(InspectorMetrics::HEADER_PADDING_LEFT)
                .pr(InspectorMetrics::HEADER_PADDING_RIGHT)
                .border_b_1()
                .border_color(theme.border)
                .child(
                    div().flex_1().min_w_0().truncate().child(
                        Text::body(title)
                            .color(ChromeColors::strong(&theme))
                            .font_weight(FontWeight::BOLD),
                    ),
                )
                .child(
                    Button::new(
                        "workspace-inspector-close",
                        dbflux_i18n::t!("document.data.row_inspector.action.close"),
                    )
                    .icon(AppIcon::CircleX)
                    .icon_only()
                    .tab_stop(false)
                    .on_click(move |_, _, cx| {
                        close_entity.update(cx, |inspector, cx| {
                            inspector.close(cx);
                        });
                    }),
                )
        });

        // The rail is an island of its own after the document island: the
        // desk gap on its left, then the island with the grip over its left
        // edge. mouse_down on the grip starts the drag; move/up are owned by
        // the workspace drag mask so cursor tracking works even after the
        // cursor leaves this column.
        div()
            .id("workspace-inspector")
            .key_context(dbflux_components::key_contexts::ROW_INSPECTOR)
            .h_full()
            .w(rail_width + IslandMetrics::GAP)
            .pl(IslandMetrics::GAP)
            .flex_shrink_0()
            .flex()
            .track_focus(&self.focus_handle)
            .on_scroll_wheel(|_, _, cx| cx.stop_propagation())
            .child(
                Island::new()
                    .h_full()
                    .w(rail_width)
                    .when_some(header, |body, header| body.child(header))
                    .child(
                        div()
                            .id("workspace-inspector-body")
                            .flex_1()
                            .min_h_0()
                            .overflow_hidden()
                            .when_some(content, |el, view| el.child(view)),
                    )
                    .child(
                        div()
                            .id("workspace-inspector-grip")
                            .absolute()
                            .top_0()
                            .bottom_0()
                            .left_0()
                            .w(INSPECTOR_GRIP_WIDTH)
                            .cursor_col_resize()
                            .hover(|el| el.bg(theme.accent.opacity(0.3)))
                            .when(is_resizing, |el| el.bg(ChromeColors::tint(&theme)))
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(|this, event: &MouseDownEvent, _, cx| {
                                    this.begin_resize(event, cx);
                                }),
                            ),
                    ),
            )
    }
}

#[cfg(test)]
mod tests {
    // Explicit imports: a glob of `gpui::*` would shadow the built-in
    // `#[test]` that `#[gpui::test]` expands to.
    use super::WorkspaceInspector;
    use gpui::{
        AnyView, AppContext, Context, IntoElement, Render, TestAppContext, Window, div, px,
    };

    struct EmptyContent;

    impl Render for EmptyContent {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div()
        }
    }

    #[gpui::test]
    fn content_with_a_minimum_widens_the_rail_only_while_it_shows(cx: &mut TestAppContext) {
        let inspector = cx.new(|cx| WorkspaceInspector::new(px(380.0), cx));
        let content = cx.new(|_| EmptyContent);

        cx.update(|cx| {
            inspector.update(cx, |inspector, cx| {
                inspector.open_with(AnyView::from(content.clone()), "Builder".into(), true, cx);
                inspector.set_content_min_width(Some(px(540.0)), cx);
                assert_eq!(inspector.rail_width(), px(540.0));
                assert_eq!(inspector.width(), px(380.0));

                inspector.fake_begin_resize_at(px(1000.0), cx);
                inspector.update_resize(px(1200.0), cx);
                assert_eq!(
                    inspector.rail_width(),
                    px(540.0),
                    "the rail cannot shrink below what its content needs"
                );
                inspector.finish_resize(cx);

                inspector.open_with(AnyView::from(content.clone()), "Row".into(), true, cx);
                assert_eq!(inspector.rail_width(), inspector.width());
            });
        });
    }
}
