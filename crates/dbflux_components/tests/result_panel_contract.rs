//! TDD contract tests for `ResultPanel` state management.
//!
//! Updated for the slot-based `ResultPanel` API (Sprint A). Tests cover the
//! state invariants accessible without a render pipeline:
//!
//! 1. Panel constructs successfully with a `ViewHandle`.
//! 2. `has_mode_bar_segment_cx` reflects `available_modes.len() >= 2`.
//! 3. `has_refresh_segment` reflects `view.supports_refresh`.
//! 4. `sync_refresh_policy` updates the dropdown without panicking.
//!
//! Render-pipeline behavior (chrome row DOM, focus, event emission) is verified
//! by manual smoke tests.
//!
//! Uses `TestAppContext::single()` + plain `#[test]` — NOT `#[gpui::test]`.

use dbflux_components::result_panel::{ResultPanel, ViewHandle};
use dbflux_components::result_view::ResultViewMode;
use dbflux_core::RefreshPolicy;
use gpui::prelude::*;
use gpui::{App, Context, TestAppContext, Window, div};

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
        .supports_refresh(|_cx| false)
        .refresh_policy(|_cx| RefreshPolicy::Manual)
        .set_refresh_policy(|_policy, _cx| {})
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
        .supports_refresh(|_cx| false)
        .refresh_policy(|_cx| RefreshPolicy::Manual)
        .set_refresh_policy(|_policy, _cx| {})
        .build()
}

/// Build a `ViewHandle` with refresh support.
fn stub_handle_with_refresh(cx: &mut App, policy: RefreshPolicy) -> ViewHandle {
    let focus = cx.focus_handle();
    let stored_policy = std::sync::Arc::new(std::sync::Mutex::new(policy));
    let stored_for_get = stored_policy.clone();
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
        .supports_refresh(|_cx| true)
        .refresh_policy(move |_cx| *stored_for_get.lock().unwrap())
        .set_refresh_policy(move |p, _cx| {
            *stored_policy.lock().unwrap() = p;
        })
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

/// Refresh segment absent when `supports_refresh` is false.
#[test]
fn refresh_absent_when_not_supported() {
    let cx = TestAppContext::single();
    cx.update(|cx| {
        let panel = cx.new(|cx| ResultPanel::new(stub_handle(cx), cx));
        assert!(!panel.read(cx).has_refresh_segment());
    });
}

/// Refresh segment present when `supports_refresh` is true.
#[test]
fn refresh_present_when_supported() {
    let cx = TestAppContext::single();
    cx.update(|cx| {
        let panel =
            cx.new(|cx| ResultPanel::new(stub_handle_with_refresh(cx, RefreshPolicy::Manual), cx));
        assert!(panel.read(cx).has_refresh_segment());
    });
}

/// `refresh_dropdown_entity` matches `has_refresh_segment`.
#[test]
fn refresh_dropdown_entity_matches_has_refresh_segment() {
    let cx = TestAppContext::single();
    cx.update(|cx| {
        let panel = cx.new(|cx| ResultPanel::new(stub_handle(cx), cx));
        assert_eq!(
            panel.read(cx).has_refresh_segment(),
            panel.read(cx).refresh_dropdown_entity().is_some(),
        );

        let panel2 =
            cx.new(|cx| ResultPanel::new(stub_handle_with_refresh(cx, RefreshPolicy::Manual), cx));
        assert_eq!(
            panel2.read(cx).has_refresh_segment(),
            panel2.read(cx).refresh_dropdown_entity().is_some(),
        );
    });
}

/// `sync_refresh_policy` does not panic.
#[test]
fn sync_refresh_policy_does_not_panic() {
    let cx = TestAppContext::single();
    cx.update(|cx| {
        let panel =
            cx.new(|cx| ResultPanel::new(stub_handle_with_refresh(cx, RefreshPolicy::Manual), cx));

        panel.update(cx, |p, cx| {
            p.sync_refresh_policy(RefreshPolicy::Interval { every_secs: 10 }, cx);
        });
    });
}
