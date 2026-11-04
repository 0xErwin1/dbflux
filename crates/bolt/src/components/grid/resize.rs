//! Drag-to-resize for a grid's header columns.
//!
//! The grid host keeps its column widths in its own state. This module only
//! tracks one drag ([`ColumnResizeDrag`]), draws the header's drag strip
//! ([`column_resize_handle`]), turns pointer movement into a requested
//! width ([`track_column_resize`]) and applies that request to a width list
//! ([`apply_column_resize`]).

use std::sync::{Arc, Mutex};

use gpui::{
    App, ElementId, Hsla, InteractiveElement, IntoElement, MouseButton, MouseDownEvent, Pixels,
    Styled, Window, div, px,
};

/// Narrowest width a column can be dragged to.
pub const MIN_COLUMN_WIDTH: f32 = 50.0;

/// Width of the drag strip on a header cell's trailing edge.
pub const RESIZE_HANDLE_WIDTH: Pixels = px(6.0); // guardrail-allow: resize handle hit area, not spacing

/// A width the pointer asked a column to take, already clamped to
/// [`MIN_COLUMN_WIDTH`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ColumnResize {
    pub column: usize,
    pub width: f32,
}

#[derive(Clone, Copy, Debug)]
struct ActiveDrag {
    column: usize,
    start_x: Pixels,
    original_width: f32,
}

/// The drag in progress, shared by the header handles that start it and the
/// grid root that follows the pointer. Clones share one drag.
#[derive(Clone, Debug, Default)]
pub struct ColumnResizeDrag {
    active: Arc<Mutex<Option<ActiveDrag>>>,
}

impl ColumnResizeDrag {
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts dragging `column`, currently `width` wide, from pointer `start_x`.
    pub fn begin(&self, column: usize, start_x: Pixels, width: f32) {
        if let Ok(mut active) = self.active.lock() {
            *active = Some(ActiveDrag {
                column,
                start_x,
                original_width: width,
            });
        }
    }

    /// The width the dragged column takes with the pointer at `position_x`,
    /// or `None` when no drag is in progress.
    pub fn update(&self, position_x: Pixels) -> Option<ColumnResize> {
        let active = (*self.active.lock().ok()?)?;
        let delta: f32 = (position_x - active.start_x).into();

        Some(ColumnResize {
            column: active.column,
            width: clamp_column_width(active.original_width + delta),
        })
    }

    /// Ends the drag, if any.
    pub fn end(&self) {
        if let Ok(mut active) = self.active.lock() {
            *active = None;
        }
    }

    pub fn is_active(&self) -> bool {
        self.active.lock().is_ok_and(|active| active.is_some())
    }
}

/// `width` raised to [`MIN_COLUMN_WIDTH`] when narrower.
pub fn clamp_column_width(width: f32) -> f32 {
    width.max(MIN_COLUMN_WIDTH)
}

/// Writes the requested width into `widths`, clamped to
/// [`MIN_COLUMN_WIDTH`], and leaves every other column as it was. Returns
/// whether a column was updated, which is `false` for an index past the end.
pub fn apply_column_resize(widths: &mut [f32], resize: ColumnResize) -> bool {
    match widths.get_mut(resize.column) {
        Some(width) => {
            *width = clamp_column_width(resize.width);
            true
        }
        None => false,
    }
}

/// The drag strip a header cell places on its trailing edge. The cell must
/// be `relative()`. Pressing it starts a drag on `drag`; the move and release
/// are followed by [`track_column_resize`] on the grid root, so the drag
/// survives the pointer leaving the strip.
pub fn column_resize_handle(
    column: usize,
    width: f32,
    drag: &ColumnResizeDrag,
    hover_color: Hsla,
) -> impl IntoElement {
    let drag = drag.clone();

    div()
        .id(ElementId::from(("resize-handle", column)))
        .absolute()
        .right_0()
        .top_0()
        .bottom_0()
        .w(RESIZE_HANDLE_WIDTH)
        .cursor_col_resize()
        .hover(|style| style.bg(hover_color))
        .on_mouse_down(
            MouseButton::Left,
            move |event: &MouseDownEvent, _window, cx| {
                cx.stop_propagation();
                drag.begin(column, event.position.x, width);
            },
        )
}

/// Makes `root` follow the pointer while `drag` is active. `on_resize`
/// receives each requested width for the host to store; the release ends the
/// drag.
pub fn track_column_resize<E>(
    root: E,
    drag: &ColumnResizeDrag,
    on_resize: impl Fn(ColumnResize, &mut Window, &mut App) + 'static,
) -> E
where
    E: InteractiveElement,
{
    let drag_for_move = drag.clone();
    let drag_for_up = drag.clone();

    root.on_mouse_move(move |event, window, cx| {
        if let Some(resize) = drag_for_move.update(event.position.x) {
            on_resize(resize, window, cx);
        }
    })
    .on_mouse_up(MouseButton::Left, move |_event, _window, _cx| {
        drag_for_up.end();
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dragging_changes_the_width_by_the_pointer_delta() {
        let drag = ColumnResizeDrag::new();
        drag.begin(2, px(100.0), 120.0);

        assert_eq!(
            drag.update(px(145.0)),
            Some(ColumnResize {
                column: 2,
                width: 165.0
            })
        );
        assert_eq!(
            drag.update(px(90.0)).map(|resize| resize.width),
            Some(110.0)
        );
    }

    #[test]
    fn dragging_clamps_to_the_minimum_width() {
        let drag = ColumnResizeDrag::new();
        drag.begin(0, px(100.0), 120.0);

        assert_eq!(
            drag.update(px(-500.0)).map(|resize| resize.width),
            Some(MIN_COLUMN_WIDTH)
        );
    }

    #[test]
    fn no_width_is_reported_without_a_drag() {
        let drag = ColumnResizeDrag::new();
        assert_eq!(drag.update(px(10.0)), None);
        assert!(!drag.is_active());

        drag.begin(0, px(0.0), 80.0);
        assert!(drag.is_active());
        drag.end();
        assert_eq!(drag.update(px(10.0)), None);
    }

    #[test]
    fn clones_share_one_drag() {
        let drag = ColumnResizeDrag::new();
        let handle_side = drag.clone();
        handle_side.begin(1, px(0.0), 80.0);

        assert!(drag.is_active());
        drag.end();
        assert!(!handle_side.is_active());
    }

    #[test]
    fn applying_a_resize_leaves_other_columns_alone() {
        let mut widths = vec![100.0, 120.0, 140.0];
        let applied = apply_column_resize(
            &mut widths,
            ColumnResize {
                column: 1,
                width: 200.0,
            },
        );

        assert!(applied);
        assert_eq!(widths, vec![100.0, 200.0, 140.0]);
    }

    #[test]
    fn applying_a_resize_clamps_and_ignores_unknown_columns() {
        let mut widths = vec![100.0, 120.0];
        apply_column_resize(
            &mut widths,
            ColumnResize {
                column: 0,
                width: 3.0,
            },
        );
        assert_eq!(widths, vec![MIN_COLUMN_WIDTH, 120.0]);

        let applied = apply_column_resize(
            &mut widths,
            ColumnResize {
                column: 5,
                width: 300.0,
            },
        );
        assert!(!applied);
        assert_eq!(widths, vec![MIN_COLUMN_WIDTH, 120.0]);
    }
}
