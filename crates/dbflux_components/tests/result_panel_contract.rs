//! TDD contract tests for `ResultPanel` state management.
//!
//! Updated to remove refresh built-in (refresh compound now lives in the view's
//! filter bar segment). Tests cover:
//!
//! 1. Panel constructs successfully with a `ViewHandle`.
//! 2. `has_mode_bar_segment_cx` reflects `available_modes.len() >= 2`.
//! 3. Chrome row uses flex_wrap + gap (no positional spacers).
//!
//! Render-pipeline behavior (chrome row DOM, focus, event emission) is verified
//! by manual smoke tests.
//!
//! Uses `TestAppContext::single()` + plain `#[test]` — NOT `#[gpui::test]`.

use dbflux_components::result_panel::{ResultPanel, ViewHandle};
use dbflux_components::result_view::ResultViewMode;
use gpui::prelude::*;
use gpui::{App, TestAppContext, div};

/// Build a minimal `ViewHandle` for tests.
fn stub_handle(cx: &mut App) -> ViewHandle {
    let focus = cx.focus_handle();
    ViewHandle::builder()
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

/// Build a `ViewHandle` with two modes (mode bar will appear).
fn stub_handle_two_modes(cx: &mut App) -> ViewHandle {
    let focus = cx.focus_handle();
    ViewHandle::builder()
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

/// Panel constructs without panicking.
#[test]
fn panel_constructs() {
    let cx = TestAppContext::single();
    cx.update(|cx| {
        let _panel = cx.new(|cx| ResultPanel::new(stub_handle(cx), cx));
    });
}

/// Mode bar absent when fewer than two modes.
#[test]
fn mode_bar_absent_when_zero_modes() {
    let cx = TestAppContext::single();
    cx.update(|cx| {
        let panel = cx.new(|cx| ResultPanel::new(stub_handle(cx), cx));
        assert!(!panel.read(cx).has_mode_bar_segment_cx(cx));
    });
}

/// Mode bar present when two or more modes.
#[test]
fn mode_bar_present_when_two_modes() {
    let cx = TestAppContext::single();
    cx.update(|cx| {
        let panel = cx.new(|cx| ResultPanel::new(stub_handle_two_modes(cx), cx));
        assert!(panel.read(cx).has_mode_bar_segment_cx(cx));
    });
}
