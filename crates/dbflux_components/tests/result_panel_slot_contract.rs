//! TDD contract tests for the slot-based `ResultPanel` (Sprint A).
//!
//! Tests cover:
//! 1. `ResultPanel::new(view_handle, cx)` constructs successfully.
//! 2. Segments added at different positions sort correctly (Left < Center <
//!    Right; within position, by index).
//! 3. Mode bar built-in segment only appears when `view.available_modes().len() > 1`.
//! 4. `ViewHandle` construction with closures; `focus_handle` accessor.
//! 5. `ViewHandle::toolbar_segments()` closures are called and merged in order.
//! 6. `all_sorted_segment_positions` returns mode bar + view segments in order.
//!
//! Note: refresh is no longer a ResultPanel built-in — the compound lives in the
//! view's filter bar segment (DataGridPanel::render_filter_bar_as_segment).
//!
//! Uses `TestAppContext::single()` + plain `#[test]` — NOT `#[gpui::test]`.
//! See the project note: `#[gpui::test]` in this crate causes rustc SIGSEGV
//! when the lib already has 9+ such expansions.

use dbflux_components::result_panel::{ResultPanel, SegmentPosition, ToolbarSegment};
use dbflux_components::result_view::ResultViewMode;
use gpui::prelude::*;
use gpui::{App, FocusHandle, TestAppContext, div};
use std::sync::{Arc, Mutex};

// ── Stub ViewHandle builders ──────────────────────────────────────────────────

/// Minimal stub: zero modes, no toolbar segments.
fn stub_view_handle(cx: &mut App) -> dbflux_components::result_panel::ViewHandle {
    let focus = cx.focus_handle();
    dbflux_components::result_panel::ViewHandle::builder()
        .render(|_w, _cx| div().into_any())
        .focus({
            let focus = focus.clone();
            move |w, _cx| {
                focus.focus(w);
            }
        })
        .focus_handle(move |_cx| focus.clone())
        .toolbar_segments(|_cx| vec![])
        .available_modes(|_cx| vec![])
        .current_mode(|_cx| ResultViewMode::Table)
        .set_mode(|_mode, _cx| {})
        .build()
}

/// Stub with two modes (Table + Chart) — mode bar should appear.
fn stub_view_handle_two_modes(cx: &mut App) -> dbflux_components::result_panel::ViewHandle {
    let focus = cx.focus_handle();
    dbflux_components::result_panel::ViewHandle::builder()
        .render(|_w, _cx| div().into_any())
        .focus({
            let focus = focus.clone();
            move |w, _cx| {
                focus.focus(w);
            }
        })
        .focus_handle(move |_cx| focus.clone())
        .toolbar_segments(|_cx| vec![])
        .available_modes(|_cx| vec![ResultViewMode::Table, ResultViewMode::Chart])
        .current_mode(|_cx| ResultViewMode::Table)
        .set_mode(|_mode, _cx| {})
        .build()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// `ResultPanel::new(view_handle, cx)` constructs without panicking.
#[test]
fn result_panel_new_constructs() {
    let cx = TestAppContext::single();
    cx.update(|cx| {
        let handle = stub_view_handle(cx);
        let _panel = cx.new(|cx| ResultPanel::new(handle, cx));
    });
}

/// Segments sort: Left before Center before Right; within a position, by index.
#[test]
fn segment_sort_order_left_center_right() {
    let cx = TestAppContext::single();
    cx.update(|cx| {
        let handle = stub_view_handle(cx);
        let panel = cx.new(|cx| {
            let mut p = ResultPanel::new(handle, cx);

            p.add_segment(ToolbarSegment {
                position: SegmentPosition::Right,
                index: 0,
                builder: Box::new(|_w, _cx| div().child("right-0").into_any()),
            });
            p.add_segment(ToolbarSegment {
                position: SegmentPosition::Left,
                index: 5,
                builder: Box::new(|_w, _cx| div().child("left-5").into_any()),
            });
            p.add_segment(ToolbarSegment {
                position: SegmentPosition::Center,
                index: 0,
                builder: Box::new(|_w, _cx| div().child("center-0").into_any()),
            });
            p.add_segment(ToolbarSegment {
                position: SegmentPosition::Left,
                index: 0,
                builder: Box::new(|_w, _cx| div().child("left-0").into_any()),
            });

            p
        });

        let order = panel.read(cx).sorted_segment_positions();

        // Left idx 0 < Left idx 5 < Center idx 0 < Right idx 0
        assert_eq!(order.len(), 4);
        assert_eq!(order[0], (SegmentPosition::Left, 0));
        assert_eq!(order[1], (SegmentPosition::Left, 5));
        assert_eq!(order[2], (SegmentPosition::Center, 0));
        assert_eq!(order[3], (SegmentPosition::Right, 0));
    });
}

/// Mode bar built-in segment is absent when `available_modes().len() < 2`.
#[test]
fn mode_bar_absent_with_zero_or_one_mode() {
    let cx = TestAppContext::single();
    cx.update(|cx| {
        let handle = stub_view_handle(cx);
        let panel = cx.new(|cx| ResultPanel::new(handle, cx));
        assert!(!panel.read(cx).has_mode_bar_segment_cx(cx));
    });
}

/// Mode bar built-in segment is present when `available_modes().len() >= 2`.
#[test]
fn mode_bar_present_with_two_or_more_modes() {
    let cx = TestAppContext::single();
    cx.update(|cx| {
        let handle = stub_view_handle_two_modes(cx);
        let panel = cx.new(|cx| ResultPanel::new(handle, cx));
        assert!(panel.read(cx).has_mode_bar_segment_cx(cx));
    });
}

/// `ViewHandle::focus_handle` accessor returns a `FocusHandle`.
#[test]
fn view_handle_focus_handle_accessible() {
    let cx = TestAppContext::single();
    cx.update(|cx| {
        let handle = stub_view_handle(cx);
        // Calling focus_handle(cx) must not panic and must return a FocusHandle.
        let fh: FocusHandle = (handle.focus_handle)(cx);
        let _: FocusHandle = fh;
    });
}

/// `ViewHandle::toolbar_segments` closures are called and returned correctly.
#[test]
fn view_handle_toolbar_segments_closure_called() {
    let called = Arc::new(Mutex::new(false));
    let cx = TestAppContext::single();

    cx.update(|cx| {
        let called_inner = called.clone();
        let focus = cx.focus_handle();
        let handle = dbflux_components::result_panel::ViewHandle::builder()
            .render(|_w, _cx| div().into_any())
            .focus({
                let focus = focus.clone();
                move |w, _cx| {
                    focus.focus(w);
                }
            })
            .focus_handle(move |_cx| focus.clone())
            .toolbar_segments(move |_cx| {
                *called_inner.lock().unwrap() = true;
                vec![]
            })
            .available_modes(|_cx| vec![])
            .current_mode(|_cx| ResultViewMode::Table)
            .set_mode(|_mode, _cx| {})
            .build();

        let _segments: Vec<ToolbarSegment> = (handle.toolbar_segments)(cx);
        assert!(*called.lock().unwrap());
    });
}

/// View's `toolbar_segments` are merged with built-in segments in correct order:
/// built-in Left (mode bar at idx 0), then view segments by (position, index).
/// When the view provides a Center segment at idx 0, the sorted order is:
/// Left/0 (mode bar), Center/0 (view filter bar — which includes the refresh compound).
///
/// Note: refresh is no longer a ResultPanel built-in — it lives inside the
/// view's Center/0 filter bar segment.
#[test]
fn merged_segments_sort_correctly() {
    let cx = TestAppContext::single();
    cx.update(|cx| {
        let focus = cx.focus_handle();
        let handle = dbflux_components::result_panel::ViewHandle::builder()
            .render(|_w, _cx| div().into_any())
            .focus({
                let focus = focus.clone();
                move |w, _cx| {
                    focus.focus(w);
                }
            })
            .focus_handle(move |_cx| focus.clone())
            // View provides one Center segment (the filter bar, which includes refresh).
            .toolbar_segments(|_cx| {
                vec![ToolbarSegment {
                    position: SegmentPosition::Center,
                    index: 0,
                    builder: Box::new(|_w, _cx| div().child("filter+refresh").into_any()),
                }]
            })
            .available_modes(|_cx| vec![ResultViewMode::Table, ResultViewMode::Chart])
            .current_mode(|_cx| ResultViewMode::Table)
            .set_mode(|_mode, _cx| {})
            .build();

        let panel = cx.new(|cx| ResultPanel::new(handle, cx));

        // Collect sorted positions via helper.
        let positions = panel.read(cx).all_sorted_segment_positions(cx);

        // Expected order: Left/0 (mode bar), Center/0 (filter bar with embedded refresh)
        assert_eq!(positions.len(), 2);
        assert_eq!(positions[0], (SegmentPosition::Left, 0u16));
        assert_eq!(positions[1], (SegmentPosition::Center, 0u16));
    });
}
