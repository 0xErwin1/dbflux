//! Shared refresh split-button rendered by both `AuditDocument` and the
//! shared chart toolbar.
//!
//! The button follows the app-standard layout:
//!   ┌─────────────────────┬─┬────────┐
//!   │ icon  label         │ │ ▾ drop │
//!   └─────────────────────┴─┴────────┘
//!
//! - When the policy is auto-refresh the icon is a clock; otherwise a
//!   circling-arrow refresh icon.
//! - The left section is clickable and calls `on_refresh`.
//! - The right section is the interval-selection dropdown.
//! - The border uses `theme.ring` when `ring` is `true`, `theme.input` otherwise.

use crate::controls::Dropdown;
use crate::icons::AppIcon;
use crate::primitives::{Icon, Text};
use crate::tokens::{Heights, Radii, Spacing};
use dbflux_core::RefreshPolicy;
use gpui::prelude::*;
use gpui::*;
use gpui_component::theme::Theme;

/// Render the refresh split-button.
///
/// `id` must be unique within the containing element tree.
/// `refresh_policy` controls the icon and caption text.
/// `ring` draws a focus-ring border around the whole outer control when `true`.
/// `ring_policy` draws an additional inner ring around the dropdown section when
/// `true` (matches the `ToolbarSlot::RefreshPolicy` ring in `AuditDocument`).
/// `refresh_dropdown` is the interval-selector dropdown entity.
/// `on_refresh` is called when the left (icon + label) action segment is clicked.
pub fn refresh_split_button(
    id: impl Into<ElementId>,
    refresh_policy: RefreshPolicy,
    ring: bool,
    ring_policy: bool,
    refresh_dropdown: Entity<Dropdown>,
    on_refresh: impl Fn(&mut Window, &mut App) + 'static,
    theme: &Theme,
) -> impl IntoElement {
    let refresh_label = if refresh_policy.is_auto() {
        refresh_policy.label()
    } else {
        "Refresh"
    };

    let refresh_icon = if refresh_policy.is_auto() {
        AppIcon::Clock
    } else {
        AppIcon::RefreshCcw
    };

    let border_color = if ring { theme.ring } else { theme.input };
    let accent = theme.accent;
    let foreground = theme.foreground;
    let input_color = theme.input;
    let ring_color = theme.ring;

    div()
        .id(id.into())
        .h(Heights::BUTTON)
        .flex()
        .items_center()
        .gap_0()
        .rounded(Radii::SM)
        .bg(theme.background)
        .border_1()
        .border_color(border_color)
        .child(
            div()
                .id("refresh-split-action")
                .h_full()
                .px(Spacing::SM)
                .flex()
                .items_center()
                .gap_1()
                .cursor_pointer()
                .hover(move |d| d.bg(accent.opacity(0.08)))
                .on_click(move |_, window, cx| {
                    on_refresh(window, cx);
                })
                .child(
                    Icon::new(refresh_icon)
                        .size(Heights::ICON_SM)
                        .color(foreground),
                )
                .child(Text::caption(refresh_label)),
        )
        .child(div().w(px(1.0)).h_full().bg(input_color)) // guardrail-allow: 1px separator div width
        .child(
            div()
                .w(px(28.0)) // guardrail-allow: dropdown panel width, not a spacing value
                .h_full()
                .rounded_r(Radii::SM)
                .when(ring_policy, |d| d.border_1().border_color(ring_color))
                .child(refresh_dropdown),
        )
}
