//! Workspace-level inspector rail.
//!
//! `WorkspaceInspector` is a persistent right-edge panel that renders arbitrary
//! `AnyView` content supplied by the event chain originating from
//! `DataGridPanel::open_row_inspector`.  The workspace owns this entity for
//! its entire lifetime; the inspector stays open across tab switches.
//!
//! # Resize
//!
//! The left-edge grip (6 px) starts the drag on `mouse_down`.  Move and up
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

use crate::ui::tokens::{Heights, Radii, Spacing};
use dbflux_components::primitives::Text;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const INSPECTOR_MIN_WIDTH: Pixels = px(240.0);
pub const INSPECTOR_MAX_WIDTH: Pixels = px(1280.0);
pub const INSPECTOR_DEFAULT_WIDTH: Pixels = px(360.0);
pub const INSPECTOR_GRIP_WIDTH: Pixels = px(6.0);

// ---------------------------------------------------------------------------
// Entity
// ---------------------------------------------------------------------------

/// Workspace-level inspector rail.
pub struct WorkspaceInspector {
    content: Option<AnyView>,
    title: SharedString,
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

    pub fn title(&self) -> &SharedString {
        &self.title
    }

    /// Open / replace the inspector content. Reuses the rail if already open.
    pub fn open_with(&mut self, content: AnyView, title: SharedString, cx: &mut Context<Self>) {
        self.content = Some(content);
        self.title = title;
        self.is_open = true;
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
        self.resize_start_width = Some(self.width);
        cx.notify();
    }

    /// Simulate a drag-start at a given x position.
    ///
    /// Equivalent to the user pressing the left mouse button on the grip at
    /// `position_x`. Provided for testing without constructing `MouseDownEvent`.
    pub fn fake_begin_resize_at(&mut self, position_x: Pixels, cx: &mut Context<Self>) {
        self.is_resizing = true;
        self.resize_start_x = Some(position_x);
        self.resize_start_width = Some(self.width);
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
        let new_width = (start_width - delta).clamp(INSPECTOR_MIN_WIDTH, INSPECTOR_MAX_WIDTH);
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
        let content_width = self.width - INSPECTOR_GRIP_WIDTH;
        let is_resizing = self.is_resizing;
        let title = self.title.clone();
        let content = self.content.clone();
        let close_entity = cx.entity().clone();

        // Build header inline to avoid split-borrow issues with render_header.
        let header = {
            let theme2 = theme.clone();
            let close_entity2 = close_entity.clone();
            div()
                .flex()
                .items_center()
                .justify_between()
                .h(Heights::TOOLBAR)
                .px(Spacing::SM)
                .flex_shrink_0()
                .border_b_1()
                .border_color(theme2.border)
                .child(Text::caption(title).color(theme2.muted_foreground))
                .child(
                    div()
                        .id("workspace-inspector-close")
                        .w(px(20.0))
                        .h(px(20.0))
                        .flex()
                        .items_center()
                        .justify_center()
                        .rounded(Radii::SM)
                        .cursor_pointer()
                        .text_color(theme2.muted_foreground)
                        .hover(move |d| d.bg(theme2.secondary).text_color(theme2.foreground))
                        .on_click(move |_, _, cx| {
                            close_entity2.update(cx, |inspector, cx| {
                                inspector.close(cx);
                            });
                        })
                        .child("\u{00d7}"),
                )
        };

        // Outer flex_row: grip (resize handle) + body (header + content host).
        div()
            .id("workspace-inspector")
            .h_full()
            .w(self.width)
            .flex_shrink_0()
            .flex()
            .flex_row()
            .bg(theme.background)
            .border_l_1()
            .border_color(theme.border)
            .track_focus(&self.focus_handle)
            .on_scroll_wheel(|_, _, cx| cx.stop_propagation())
            // Grip (left edge, INSPECTOR_GRIP_WIDTH px).
            // mouse_down starts the drag; move/up are owned by the workspace drag mask
            // so cursor tracking works even after the cursor leaves this column.
            .child(
                div()
                    .id("workspace-inspector-grip")
                    .h_full()
                    .w(INSPECTOR_GRIP_WIDTH)
                    .flex_shrink_0()
                    .cursor_col_resize()
                    .hover(|el| el.bg(theme.accent.opacity(0.3)))
                    .when(is_resizing, |el| el.bg(theme.primary))
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(|this, event: &MouseDownEvent, _, cx| {
                            this.begin_resize(event, cx);
                        }),
                    ),
            )
            .child(
                div()
                    .h_full()
                    .w(content_width)
                    .flex()
                    .flex_col()
                    .overflow_hidden()
                    .child(header)
                    .child(
                        div()
                            .id("workspace-inspector-body")
                            .flex_1()
                            .min_h_0()
                            .overflow_hidden()
                            .when_some(content, |el, view| el.child(view)),
                    ),
            )
    }
}
