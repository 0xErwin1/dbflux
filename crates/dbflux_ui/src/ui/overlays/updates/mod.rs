//! The "What is new" and first-run welcome dialogs.
//!
//! Both are `modals::Modal` cards drawn with the card cut (14) the boards
//! use. They render the bundled changelog and write the two update
//! preferences straight to storage when a checkbox changes, so closing the
//! dialog never loses a choice. The changelog list shared by the two dialogs
//! lives here.

pub mod welcome;
pub mod whats_new;

pub use welcome::WelcomeDialog;
pub use whats_new::WhatsNewDialog;

use dbflux_app::updates::{ChangelogRelease, ChangelogSection, ReleaseHeading, SectionKind};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{ChromeColors, FontSizes, Spacing};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// Vertical padding of one changelog entry.
const ENTRY_PADDING_Y: Pixels = px(6.0);

/// Size of the icon in front of each changelog entry.
const ENTRY_ICON_SIZE: Pixels = px(15.0);

/// Which update preference a dialog checkbox writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UpdatePreference {
    CheckOnStartup,
    ShowWhatsNew,
}

/// Persists one update preference, reporting a storage failure to the user
/// who just toggled it.
pub(crate) fn set_update_preference(
    app_state: &Entity<AppStateEntity>,
    preference: UpdatePreference,
    value: bool,
    cx: &mut App,
) {
    let result = app_state.update(cx, |state, cx| {
        let mut settings = state.update_settings().clone();
        match preference {
            UpdatePreference::CheckOnStartup => settings.check_for_updates_on_startup = value,
            UpdatePreference::ShowWhatsNew => settings.show_whats_new_after_update = value,
        }

        let result = state.set_update_settings(settings);
        cx.emit(dbflux_ui_base::AppStateChanged);
        cx.notify();
        result
    });

    if let Err(error) = result {
        report_error(
            UserFacingError::new(
                ErrorKind::Storage,
                dbflux_i18n::t!("updates.settings.save_error", error = error),
            ),
            cx,
        );
    }
}

/// Section label in the Expanded display face, with a leading icon.
pub(crate) fn labeled_heading(icon: AppIcon, icon_color: Hsla, label: String) -> Div {
    div()
        .flex()
        .items_center()
        .gap(Spacing::SM)
        .child(Icon::new(icon).size(ENTRY_ICON_SIZE).color(icon_color))
        .child(Text::label(label))
}

fn section_label(kind: &SectionKind) -> String {
    match kind {
        SectionKind::Added => dbflux_i18n::t!("updates.sections.added"),
        SectionKind::Changed => dbflux_i18n::t!("updates.sections.changed"),
        SectionKind::Deprecated => dbflux_i18n::t!("updates.sections.deprecated"),
        SectionKind::Removed => dbflux_i18n::t!("updates.sections.removed"),
        SectionKind::Fixed => dbflux_i18n::t!("updates.sections.fixed"),
        SectionKind::Security => dbflux_i18n::t!("updates.sections.security"),
        SectionKind::Other(heading) => heading.clone(),
    }
}

fn section_icon(kind: &SectionKind, theme: &gpui_component::Theme) -> (AppIcon, Hsla) {
    match kind {
        SectionKind::Added => (AppIcon::Plus, ChromeColors::tint(theme)),
        SectionKind::Changed => (AppIcon::Pencil, ChromeColors::tint(theme)),
        SectionKind::Fixed => (AppIcon::Check, theme.success),
        SectionKind::Removed => (AppIcon::Minus, theme.danger),
        SectionKind::Deprecated | SectionKind::Security => (AppIcon::TriangleAlert, theme.warning),
        SectionKind::Other(_) => (AppIcon::ChevronRight, theme.muted_foreground),
    }
}

/// The name a release is shown under: its version, or "Unreleased".
pub(crate) fn release_label(release: &ChangelogRelease) -> String {
    match &release.heading {
        ReleaseHeading::Version(version) => version.to_string(),
        ReleaseHeading::Unreleased => dbflux_i18n::t!("updates.whats_new.unreleased"),
    }
}

/// `0.8.0  2026-10-15`: the release version in mono and its date.
pub(crate) fn release_header(release: &ChangelogRelease, cx: &App) -> Div {
    let theme = cx.theme();

    div()
        .flex()
        .items_baseline()
        .gap(Spacing::MD)
        .child(
            Text::code(release_label(release))
                .font_weight(FontWeight::BOLD)
                .text_color(theme.accent_foreground),
        )
        .when_some(release.date.clone(), |row, date| {
            row.child(Text::caption(date))
        })
}

/// A changelog section: its labeled heading and up to `limit` entries.
pub(crate) fn render_section(section: &ChangelogSection, limit: usize, cx: &App) -> Div {
    let theme = cx.theme();
    let (icon, icon_color) = section_icon(&section.kind, theme);

    let heading = div()
        .flex()
        .items_center()
        .gap(Spacing::XXS)
        .pt(Spacing::XXS)
        .child(Icon::new(icon).size(FontSizes::XS).color(icon_color))
        .child(Text::label(section_label(&section.kind)));

    let entries = section.entries.iter().take(limit).map(|entry| {
        div()
            .flex()
            .items_start()
            .gap(Spacing::MD)
            .py(ENTRY_PADDING_Y)
            .child(
                Icon::new(AppIcon::ChevronRight)
                    .size(ENTRY_ICON_SIZE)
                    .color(theme.muted_foreground),
            )
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .flex()
                    .flex_col()
                    .gap(px(2.0))
                    .child(Text::body(entry.title.clone()).text_color(theme.accent_foreground))
                    .when_some(entry.summary.clone(), |column, summary| {
                        column.child(Text::caption(summary))
                    }),
            )
    });

    div().flex().flex_col().child(heading).children(entries)
}

/// A checkbox row with a label and optional helper text below it.
pub(crate) fn preference_row(
    id: &'static str,
    label: String,
    hint: Option<String>,
    checked: bool,
    on_toggle: impl Fn(bool, &mut Window, &mut App) + 'static,
) -> Div {
    div()
        .flex()
        .items_start()
        .gap(Spacing::MD)
        .py(Spacing::XXS)
        .child(
            dbflux_components::controls::Checkbox::new(id)
                .checked(checked)
                .aria_label(label.clone())
                .on_click(move |value: &bool, window, cx| on_toggle(*value, window, cx)),
        )
        .child(
            div()
                .flex()
                .flex_col()
                .gap(px(3.0))
                .child(Text::body(label))
                .when_some(hint, |column, hint| column.child(Text::caption(hint))),
        )
}

#[cfg(test)]
mod tests {
    // Explicit imports: the parent's `gpui::*` glob would make `#[test]`
    // resolve to `gpui::test`, whose expansion recurses without bound.
    use super::{SectionKind, section_label};

    #[test]
    fn section_labels_resolve_in_every_locale() {
        for key in [
            "updates.sections.added",
            "updates.sections.changed",
            "updates.sections.deprecated",
            "updates.sections.removed",
            "updates.sections.fixed",
            "updates.sections.security",
        ] {
            for locale in ["en", "es", "ko", "zh_Hans"] {
                let value = dbflux_i18n::t!(key, locale = locale);
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing in {locale}"
                );
            }
        }

        assert_eq!(
            section_label(&SectionKind::Other("Chores".to_string())),
            "Chores"
        );
    }
}
