//! `ResultPanel` — shared chrome host for all result-rendering views.
//!
//! `ResultPanel` owns the mode bar (Table / Chart / Json / Text / Raw tabs) and
//! optionally the refresh dropdown, then delegates content rendering to an inner
//! `AnyView` child. The inner child is responsible for the data table, document
//! tree, pagination, footer/status bar, and all mutation controls.
//!
//! ## Mode-change events
//!
//! When the user clicks a mode tab, `ResultPanel` emits
//! `ResultPanelEvent::ModeChanged`. The container document (e.g. `DataDocument`)
//! subscribes and forwards the new mode to the underlying grid or view entity.
//!
//! ## Pixel-equivalence note
//!
//! The mode bar rendered here replaces the `render_result_tabs_strip` that
//! previously lived inside `DataGridPanel::render`. The strip now appears
//! above the child view in the outer `ResultPanel` flex-column. The refresh
//! dropdown is also rendered here (right-aligned in the mode bar row) rather
//! than inside `DataGridPanel::render_toolbar`. The same dropdown entity is
//! shared with the underlying `DataGridPanel` so the chart toolbar can render
//! it in the chart-specific chrome row when the chart view is active.

use crate::controls::{Dropdown, DropdownItem, DropdownSelectionChanged};
use crate::result_view::ResultViewMode;
use crate::tokens::{FontSizes, Spacing};
use dbflux_core::RefreshPolicy;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

// ── Events ───────────────────────────────────────────────────────────────────

/// Events emitted by `ResultPanel`.
///
/// Container documents subscribe to these to forward mode / policy changes to
/// the underlying data view entity.
#[derive(Clone, Debug)]
pub enum ResultPanelEvent {
    /// The user clicked a mode tab in the mode bar.
    ModeChanged(ResultViewMode),
    /// The user changed the refresh policy via the dropdown.
    RefreshPolicyChanged(RefreshPolicy),
}

// ── Struct ────────────────────────────────────────────────────────────────────

/// Shared chrome host for all result-rendering views.
///
/// Owns: mode bar, focus root, and (optionally) the refresh-policy dropdown.
/// Delegates content rendering to the inner `view` child.
///
/// The `available_modes` list is set by the container document after each
/// query run, so the mode bar reflects the current result shape.
pub struct ResultPanel {
    /// The inner content view. Renders the actual table/chart/document tree.
    view: AnyView,
    /// Modes shown in the tab bar. Empty list means the bar is hidden.
    available_modes: Vec<ResultViewMode>,
    /// Currently active mode. Must be one of `available_modes` (or Table if
    /// the list is empty).
    current_mode: ResultViewMode,
    /// Current refresh policy, mirrored from the underlying data entity.
    refresh_policy: RefreshPolicy,
    /// Optional refresh-policy dropdown. Created by `new_with_refresh` when
    /// the panel should own refresh chrome. Rendered in the mode bar row when
    /// `Some`; shared with the underlying data grid for chart toolbar rendering.
    refresh_dropdown: Option<Entity<Dropdown>>,
    /// Focus root for the panel; children receive focus via `focus_handle`.
    focus_handle: FocusHandle,
    /// Subscription to the refresh dropdown (kept alive alongside the dropdown).
    _refresh_sub: Option<Subscription>,
}

impl ResultPanel {
    // ── Constructors ─────────────────────────────────────────────────────────

    /// Create a `ResultPanel` wrapping `view`, with no mode bar (modes added
    /// later via `set_available_modes`) and no refresh dropdown.
    pub fn new(
        view: AnyView,
        available_modes: Vec<ResultViewMode>,
        cx: &mut Context<Self>,
    ) -> Self {
        let current_mode = available_modes
            .first()
            .copied()
            .unwrap_or(ResultViewMode::Table);

        Self {
            view,
            available_modes,
            current_mode,
            refresh_policy: RefreshPolicy::Manual,
            refresh_dropdown: None,
            focus_handle: cx.focus_handle(),
            _refresh_sub: None,
        }
    }

    /// Create a `ResultPanel` that also owns a refresh-policy dropdown.
    ///
    /// `default_policy` is the initially selected policy.
    /// `supports_auto` — when false, auto-refresh items are disabled.
    pub fn new_with_refresh(
        view: AnyView,
        available_modes: Vec<ResultViewMode>,
        default_policy: RefreshPolicy,
        supports_auto: bool,
        cx: &mut Context<Self>,
    ) -> Self {
        let items: Vec<DropdownItem> = RefreshPolicy::ALL
            .iter()
            .map(|p| DropdownItem::new(p.label()))
            .collect();

        let dropdown = cx.new(|_cx| {
            Dropdown::new("result-panel-refresh")
                .items(items)
                .selected_index(Some(default_policy.index()))
                .disabled(!supports_auto)
                .compact_trigger(true)
        });

        let sub = cx.subscribe(
            &dropdown,
            |this, _, event: &DropdownSelectionChanged, cx| {
                let policy = RefreshPolicy::from_index(event.index);
                this.refresh_policy = policy;
                cx.emit(ResultPanelEvent::RefreshPolicyChanged(policy));
                cx.notify();
            },
        );

        let current_mode = available_modes
            .first()
            .copied()
            .unwrap_or(ResultViewMode::Table);

        Self {
            view,
            available_modes,
            current_mode,
            refresh_policy: default_policy,
            refresh_dropdown: Some(dropdown),
            focus_handle: cx.focus_handle(),
            _refresh_sub: Some(sub),
        }
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    pub fn available_modes(&self) -> &[ResultViewMode] {
        &self.available_modes
    }

    pub fn current_mode(&self) -> ResultViewMode {
        self.current_mode
    }

    pub fn refresh_policy(&self) -> RefreshPolicy {
        self.refresh_policy
    }

    pub fn focus_handle(&self) -> &FocusHandle {
        &self.focus_handle
    }

    /// Return the refresh dropdown entity owned by this panel, if present.
    ///
    /// The container document calls this once after construction to wire the
    /// same entity into the underlying data grid (for chart toolbar rendering
    /// and policy reset propagation).
    pub fn refresh_dropdown_entity(&self) -> Option<&Entity<Dropdown>> {
        self.refresh_dropdown.as_ref()
    }

    // ── Mutations ─────────────────────────────────────────────────────────────

    /// Replace the available modes list. Resets `current_mode` to the first
    /// available mode if the current mode is no longer in the list.
    pub fn set_available_modes(&mut self, modes: Vec<ResultViewMode>, cx: &mut Context<Self>) {
        self.available_modes = modes;

        if !self.available_modes.contains(&self.current_mode) {
            self.current_mode = self
                .available_modes
                .first()
                .copied()
                .unwrap_or(ResultViewMode::Table);
        }

        cx.notify();
    }

    /// Change the active mode without emitting a `ModeChanged` event.
    ///
    /// Used by the container document to sync the mode bar when the underlying
    /// data view changes mode internally (e.g. automatic Chart selection after
    /// a TimeSeries query run).
    pub fn set_current_mode(&mut self, mode: ResultViewMode, cx: &mut Context<Self>) {
        if self.current_mode == mode {
            return;
        }

        self.current_mode = mode;
        cx.notify();
    }

    /// Mirror the current refresh policy from the underlying data view.
    pub fn set_refresh_policy(&mut self, policy: RefreshPolicy, cx: &mut Context<Self>) {
        if self.refresh_policy == policy {
            return;
        }

        self.refresh_policy = policy;
        cx.notify();
    }

    /// Sync the stored policy and the dropdown selection without emitting a
    /// `RefreshPolicyChanged` event.
    ///
    /// Use this when the underlying data view resets its own policy internally
    /// (e.g. when a new query result arrives) and the panel must mirror that
    /// reset without triggering a round-trip back to the data view.
    pub fn sync_refresh_policy(&mut self, policy: RefreshPolicy, cx: &mut Context<Self>) {
        self.refresh_policy = policy;

        if let Some(dd) = &self.refresh_dropdown {
            dd.update(cx, |dd, cx| {
                dd.set_selected_index(Some(policy.index()), cx);
            });
        }

        cx.notify();
    }

    /// Focus the panel (delegates to the panel's own focus handle).
    pub fn focus(&self, window: &mut Window) {
        self.focus_handle.focus(window);
    }

    // ── Rendering helpers ─────────────────────────────────────────────────────

    /// Render the mode tab bar (and refresh dropdown, if present).
    ///
    /// The bar is rendered when either:
    /// - `available_modes` has two or more entries (mode tabs are shown), or
    /// - a refresh dropdown is present (it lives in the same bar row).
    ///
    /// The bar is hidden when in Chart mode because `DataGridPanel` renders its
    /// own chart-specific chrome (which includes RANGE controls, the refresh
    /// dropdown, and the clock widget).
    fn render_mode_bar(&mut self, cx: &mut Context<Self>) -> Option<impl IntoElement> {
        let has_mode_tabs = self.available_modes.len() >= 2;
        let has_refresh = self.refresh_dropdown.is_some();

        // Nothing to render.
        if !has_mode_tabs && !has_refresh {
            return None;
        }

        // Chart mode: DataGridPanel's chart toolbar owns the full chrome row
        // (RANGE presets + REFRESH + clock), so suppress this bar to avoid
        // duplication.
        if self.current_mode == ResultViewMode::Chart {
            return None;
        }

        let theme = cx.theme().clone();
        let current = self.current_mode;
        let modes = self.available_modes.clone();
        let refresh_dropdown = self.refresh_dropdown.clone();

        let bar = div()
            .flex()
            .flex_row()
            .items_center()
            .h(px(30.0))
            .px(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            // Mode tabs (shown only when 2+ modes are available).
            .when(has_mode_tabs, |d| {
                d.children(modes.into_iter().enumerate().map(|(i, mode)| {
                    let is_active = mode == current;

                    div()
                        .id(ElementId::Name(format!("result-panel-mode-{}", i).into()))
                        .flex()
                        .items_center()
                        .gap(Spacing::XS)
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
                            cx.listener(move |this, _, _, cx| {
                                if this.current_mode != mode {
                                    this.current_mode = mode;
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
            })
            // Spacer: push the refresh dropdown to the right edge.
            .when(has_mode_tabs && has_refresh, |d| d.child(div().flex_1()))
            // Refresh dropdown (compact-trigger size: 28 px wide, full bar height).
            .when_some(refresh_dropdown, |d, dropdown| {
                d.child(div().w(px(28.0)).h_full().child(dropdown))
            });

        Some(bar)
    }
}

// ── EventEmitter ──────────────────────────────────────────────────────────────

impl EventEmitter<ResultPanelEvent> for ResultPanel {}

// ── Render ────────────────────────────────────────────────────────────────────

impl Render for ResultPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Pre-clone to avoid simultaneous borrow conflict between render_mode_bar
        // (which mutably borrows self via cx.listener) and track_focus below.
        let focus_handle = self.focus_handle.clone();
        let view = self.view.clone();
        let mode_bar = self.render_mode_bar(cx);

        div()
            .track_focus(&focus_handle)
            .flex()
            .flex_col()
            .size_full()
            .when_some(mode_bar, |d, bar| d.child(bar))
            .child(view)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────
//
// State contract tests live in `crates/dbflux_components/tests/result_panel_contract.rs`
// as integration tests compiled as a separate crate. This avoids the rustc
// SIGSEGV caused by `#[gpui::test]` proc-macro recursion overflow when the
// lib test binary already contains many such expansions from other modules.
