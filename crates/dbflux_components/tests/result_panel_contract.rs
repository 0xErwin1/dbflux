//! TDD contract tests for `ResultPanel` state management.
//!
//! Placed in the integration test directory (compiled as a separate crate) to
//! avoid the rustc SIGSEGV caused by `#[gpui::test]` proc-macro recursion
//! overflow when the lib test binary already contains many such macro
//! expansions from other modules.
//!
//! Covers:
//! 1. `new(view, modes, cx)` — first mode becomes `current_mode`;
//!    `available_modes()` returns the supplied list; empty → defaults to Table.
//! 2. `set_current_mode(mode)` — changes the active mode; no-op on same value.
//! 3. `set_refresh_policy(policy)` — updates the stored policy.
//! 4. `available_modes()` — returns the configured list unchanged.
//!
//! Render-pipeline behaviour (mode bar DOM, focus, event emission) requires a
//! full frame paint and is verified by manual smoke tests.

use dbflux_components::result_panel::ResultPanel;
use dbflux_components::result_view::ResultViewMode;
use dbflux_core::RefreshPolicy;
use gpui::prelude::*;
use gpui::{AnyView, Context, TestAppContext, Window, div};

/// Minimal stub view used as the inner child for ResultPanel in tests.
struct StubView;

impl Render for StubView {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
    }
}

fn stub_view(cx: &mut gpui::App) -> AnyView {
    AnyView::from(cx.new(|_cx| StubView))
}

/// New panel: first available mode is selected as `current_mode`.
#[test]
fn new_selects_first_available_mode() {
    let cx = TestAppContext::single();

    cx.update(|cx| {
        let modes = vec![
            ResultViewMode::Table,
            ResultViewMode::Chart,
            ResultViewMode::Json,
        ];
        let panel = cx.new(|cx| ResultPanel::new(stub_view(cx), modes.clone(), cx));

        assert_eq!(panel.read(cx).current_mode(), ResultViewMode::Table);
        assert_eq!(panel.read(cx).available_modes(), modes.as_slice());
    });
}

/// New panel with empty mode list: `current_mode` falls back to Table.
#[test]
fn new_with_empty_modes_defaults_to_table() {
    let cx = TestAppContext::single();

    cx.update(|cx| {
        let panel = cx.new(|cx| ResultPanel::new(stub_view(cx), vec![], cx));
        assert_eq!(panel.read(cx).current_mode(), ResultViewMode::Table);
    });
}

/// `set_current_mode` changes the active mode.
#[test]
fn set_current_mode_changes_mode() {
    let cx = TestAppContext::single();

    cx.update(|cx| {
        let modes = vec![ResultViewMode::Table, ResultViewMode::Json];
        let panel = cx.new(|cx| ResultPanel::new(stub_view(cx), modes, cx));

        panel.update(cx, |p, cx| p.set_current_mode(ResultViewMode::Json, cx));
        assert_eq!(panel.read(cx).current_mode(), ResultViewMode::Json);
    });
}

/// `set_current_mode` with the same value is a no-op (mode is unchanged).
#[test]
fn set_current_mode_noop_on_same_value() {
    let cx = TestAppContext::single();

    cx.update(|cx| {
        let modes = vec![ResultViewMode::Table, ResultViewMode::Json];
        let panel = cx.new(|cx| ResultPanel::new(stub_view(cx), modes, cx));

        // move to Json first
        panel.update(cx, |p, cx| p.set_current_mode(ResultViewMode::Json, cx));
        // call again with same value
        panel.update(cx, |p, cx| p.set_current_mode(ResultViewMode::Json, cx));
        assert_eq!(panel.read(cx).current_mode(), ResultViewMode::Json);
    });
}

/// `set_refresh_policy` updates the stored policy.
#[test]
fn set_refresh_policy_updates_policy() {
    let cx = TestAppContext::single();

    cx.update(|cx| {
        let panel = cx.new(|cx| ResultPanel::new(stub_view(cx), vec![], cx));
        assert_eq!(panel.read(cx).refresh_policy(), RefreshPolicy::Manual);

        panel.update(cx, |p, cx| {
            p.set_refresh_policy(RefreshPolicy::Interval { every_secs: 5 }, cx);
        });
        assert!(matches!(
            panel.read(cx).refresh_policy(),
            RefreshPolicy::Interval { every_secs: 5 }
        ));
    });
}

/// `available_modes` returns the list originally passed to `new`.
#[test]
fn available_modes_returns_configured_list() {
    let cx = TestAppContext::single();

    cx.update(|cx| {
        let modes = vec![ResultViewMode::Table, ResultViewMode::Json];
        let panel = cx.new(|cx| ResultPanel::new(stub_view(cx), modes.clone(), cx));
        assert_eq!(panel.read(cx).available_modes(), modes.as_slice());
    });
}

/// `new_with_refresh` stores the default policy and creates a dropdown entity.
#[test]
fn new_with_refresh_stores_policy_and_creates_dropdown() {
    let cx = TestAppContext::single();

    cx.update(|cx| {
        let default = RefreshPolicy::Interval { every_secs: 5 };
        let panel =
            cx.new(|cx| ResultPanel::new_with_refresh(stub_view(cx), vec![], default, true, cx));

        assert!(matches!(
            panel.read(cx).refresh_policy(),
            RefreshPolicy::Interval { every_secs: 5 }
        ));

        assert!(
            panel.read(cx).refresh_dropdown_entity().is_some(),
            "dropdown entity should be present after new_with_refresh"
        );
    });
}

/// `new` (without refresh) has no dropdown entity.
#[test]
fn new_without_refresh_has_no_dropdown_entity() {
    let cx = TestAppContext::single();

    cx.update(|cx| {
        let panel = cx.new(|cx| ResultPanel::new(stub_view(cx), vec![], cx));
        assert!(
            panel.read(cx).refresh_dropdown_entity().is_none(),
            "no dropdown entity expected for plain new()"
        );
    });
}

/// `sync_refresh_policy` updates the stored policy without emitting an event.
#[test]
fn sync_refresh_policy_updates_policy() {
    let cx = TestAppContext::single();

    cx.update(|cx| {
        let panel = cx.new(|cx| {
            ResultPanel::new_with_refresh(stub_view(cx), vec![], RefreshPolicy::Manual, true, cx)
        });

        panel.update(cx, |p, cx| {
            p.sync_refresh_policy(RefreshPolicy::Interval { every_secs: 10 }, cx);
        });

        assert!(matches!(
            panel.read(cx).refresh_policy(),
            RefreshPolicy::Interval { every_secs: 10 }
        ));
    });
}

/// `set_refresh_policy` updates policy state (inherited from existing tests;
/// included here to verify the method remains accessible after the refresh
/// constructor was added).
#[test]
fn set_refresh_policy_updates_policy_with_refresh_ctor() {
    let cx = TestAppContext::single();

    cx.update(|cx| {
        let panel = cx.new(|cx| {
            ResultPanel::new_with_refresh(stub_view(cx), vec![], RefreshPolicy::Manual, true, cx)
        });

        panel.update(cx, |p, cx| {
            p.set_refresh_policy(RefreshPolicy::Interval { every_secs: 30 }, cx);
        });

        assert!(matches!(
            panel.read(cx).refresh_policy(),
            RefreshPolicy::Interval { every_secs: 30 }
        ));
    });
}
