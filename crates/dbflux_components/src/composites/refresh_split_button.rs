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

/// Translated label for a [`RefreshPolicy`].
///
/// A named interval renders its seconds directly (`"{every_secs}s"`): the
/// suffix is a unit symbol, not prose, so it stays outside the catalog.
/// Manual and any interval outside [`RefreshPolicy::ALL`] resolve through
/// the `document.shared.refresh.*` catalog entries.
pub fn refresh_policy_label(policy: RefreshPolicy) -> String {
    match policy {
        RefreshPolicy::Manual => dbflux_i18n::t!("document.shared.refresh.off"),
        RefreshPolicy::Interval { every_secs } if RefreshPolicy::ALL.contains(&policy) => {
            format!("{every_secs}s")
        }
        RefreshPolicy::Interval { .. } => dbflux_i18n::t!("document.shared.refresh.custom"),
    }
}

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
    let refresh_label: SharedString = if refresh_policy.is_auto() {
        refresh_policy_label(refresh_policy).into()
    } else {
        dbflux_i18n::t!("composites.refresh_split_button.label").into()
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

#[cfg(test)]
mod tests {
    use super::refresh_policy_label;
    use dbflux_core::RefreshPolicy;

    #[test]
    fn refresh_policy_label_maps_every_variant_to_its_catalog_entry() {
        for policy in RefreshPolicy::ALL {
            let label = refresh_policy_label(*policy);

            match policy {
                RefreshPolicy::Manual => {
                    assert_eq!(label, dbflux_i18n::t!("document.shared.refresh.off"))
                }
                RefreshPolicy::Interval { every_secs } => {
                    assert_eq!(label, format!("{every_secs}s"))
                }
            }
        }

        assert_eq!(
            refresh_policy_label(RefreshPolicy::Interval { every_secs: 7 }),
            dbflux_i18n::t!("document.shared.refresh.custom")
        );
    }

    #[test]
    fn refresh_policy_keys_resolve_in_every_locale() {
        for key in [
            "document.shared.refresh.off",
            "document.shared.refresh.custom",
        ] {
            for locale in ["en", "es", "ko", "zh_Hans"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(
                    !value.is_empty() && value != format!("{locale}.{key}"),
                    "{key} missing in {locale}"
                );
            }
        }

        assert_ne!(
            dbflux_i18n::t!("document.shared.refresh.off", locale = "en"),
            dbflux_i18n::t!("document.shared.refresh.off", locale = "es")
        );
    }

    #[test]
    fn refresh_split_button_label_key_resolves() {
        let en = dbflux_i18n::t!("composites.refresh_split_button.label", locale = "en");
        let es = dbflux_i18n::t!("composites.refresh_split_button.label", locale = "es");

        assert!(!en.is_empty() && en != "composites.refresh_split_button.label");
        assert!(!es.is_empty() && es != "composites.refresh_split_button.label");
        assert_ne!(en, es);
    }
}
