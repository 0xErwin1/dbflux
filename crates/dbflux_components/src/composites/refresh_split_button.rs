//! Refresh split button shared by `AuditDocument`, the chart toolbar, and the
//! dashboard toolbar: a [`SplitButton`] whose main action refreshes now and
//! whose menu segment hosts the auto-refresh interval dropdown.
//!
//! The main action shows a clock and the interval while auto-refresh is on,
//! otherwise a circling arrow and "Refresh".

use crate::composites::SplitButton;
use crate::controls::{Button, Dropdown};
use crate::icons::AppIcon;
use dbflux_core::RefreshPolicy;
use gpui::*;

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

/// Render the refresh split button.
///
/// `id` must be unique within the containing element tree. `focused` draws the
/// focus ring around the main action and `menu_focused` around the dropdown
/// segment, for toolbars that track keyboard slots themselves. `on_refresh`
/// runs when the main action is clicked or activated from the keyboard.
pub fn refresh_split_button(
    id: impl Into<ElementId>,
    refresh_policy: RefreshPolicy,
    focused: bool,
    menu_focused: bool,
    refresh_dropdown: Entity<Dropdown>,
    on_refresh: impl Fn(&mut Window, &mut App) + 'static,
) -> SplitButton {
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

    let main = Button::new("refresh-action", refresh_label)
        .small()
        .icon(refresh_icon)
        .focused(focused)
        .on_click(move |_, window, cx| on_refresh(window, cx));

    SplitButton::new(id, main, refresh_dropdown).menu_focused(menu_focused)
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
