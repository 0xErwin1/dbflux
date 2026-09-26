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

use dbflux_app::updates::{self, ChangelogRelease, ChangelogSection, ReleaseHeading, SectionKind};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{ChromeColors, FontSizes, Spacing};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::Scrollbar;

/// Size of the icon in front of each changelog entry and column heading.
const ENTRY_ICON_SIZE: Pixels = px(15.0);

/// Size of the icon in front of a changelog section label.
const SECTION_ICON_SIZE: Pixels = FontSizes::XS;

/// Size of the Expanded section and column labels on the boards.
const LABEL_SIZE: Pixels = px(10.0);

/// Gap between a section icon and its label.
const SECTION_HEADING_GAP: Pixels = px(7.0);

/// Gap between an entry icon and its text, and between a checkbox and its
/// label.
const ROW_GAP: Pixels = px(10.0);

/// Gap between an entry title and its summary.
const ENTRY_TEXT_GAP: Pixels = px(2.0);

/// Gap between a checkbox label and its helper text.
const HINT_GAP: Pixels = px(3.0);

/// Vertical padding of a checkbox row.
const PREFERENCE_PADDING_Y: Pixels = Spacing::XXS;

/// Text size of the "Full changelog" link and the footer note.
pub(crate) const LINK_TEXT_SIZE: Pixels = px(12.5);

/// Size of the "Full changelog" link icon.
const LINK_ICON_SIZE: Pixels = px(13.0);

/// Gap between the "Full changelog" link icon and its text.
const LINK_GAP: Pixels = Spacing::XXS;

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

/// Column label in the Expanded display face, with a leading tinted icon.
pub(crate) fn labeled_heading(icon: AppIcon, icon_color: Hsla, label: String) -> Div {
    div()
        .flex()
        .items_center()
        .gap(Spacing::SM)
        .child(Icon::new(icon).size(ENTRY_ICON_SIZE).color(icon_color))
        .child(Text::label(label).font_size(LABEL_SIZE))
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

/// `0.8.0  2026-10-15`: the release version in strong mono and its date.
pub(crate) fn release_header(release: &ChangelogRelease, cx: &App) -> Div {
    div()
        .flex()
        .items_baseline()
        .gap(ROW_GAP)
        .child(
            Text::code(release_label(release))
                .font_size(FontSizes::SM)
                .font_weight(FontWeight::BOLD)
                .text_color(ChromeColors::strong(cx.theme())),
        )
        .when_some(release.date.clone(), |row, date| {
            row.child(Text::caption(date))
        })
}

/// A changelog section: its labeled heading and up to `limit` entries, each
/// padded by `entry_padding_y` above and below.
///
/// An entry is the section icon, its title and one muted summary line that
/// ends in an ellipsis when it does not fit; the title wraps.
pub(crate) fn render_section(
    section: &ChangelogSection,
    limit: usize,
    entry_padding_y: Pixels,
    cx: &App,
) -> Div {
    let theme = cx.theme();
    let (icon, icon_color) = section_icon(&section.kind, theme);
    let strong = ChromeColors::strong(theme);
    let muted = theme.muted_foreground;

    let heading = div()
        .flex()
        .items_center()
        .gap(SECTION_HEADING_GAP)
        .pt(Spacing::XXS)
        .child(Icon::new(icon).size(SECTION_ICON_SIZE).color(icon_color))
        .child(Text::label(section_label(&section.kind)).font_size(LABEL_SIZE));

    let entries = section.entries.iter().take(limit).map(|entry| {
        div()
            .flex()
            .items_start()
            .gap(ROW_GAP)
            .py(entry_padding_y)
            .child(Icon::new(icon).size(ENTRY_ICON_SIZE).color(muted))
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .flex()
                    .flex_col()
                    .gap(ENTRY_TEXT_GAP)
                    .child(Text::body(entry.title.clone()).text_color(strong))
                    .when_some(entry.summary.clone(), |column, summary| {
                        column.child(div().min_w_0().truncate().child(Text::caption(summary)))
                    }),
            )
    });

    div().flex().flex_col().child(heading).children(entries)
}

/// The "Full changelog" link: tinted text with an external-link icon.
pub(crate) fn full_changelog_link(id: &'static str, cx: &App) -> Stateful<Div> {
    let tint = ChromeColors::tint(cx.theme());

    div()
        .id(id)
        .flex()
        .items_center()
        .gap(LINK_GAP)
        .cursor_pointer()
        .text_size(LINK_TEXT_SIZE)
        .text_color(tint)
        .hover(|link| link.underline())
        .on_click(|_, _, cx| cx.open_url(updates::FULL_CHANGELOG_URL))
        .child(
            Icon::new(AppIcon::ExternalLink)
                .size(LINK_ICON_SIZE)
                .color(tint),
        )
        .child(dbflux_i18n::t!("updates.welcome.full_changelog"))
}

/// A vertically scrolling region that `handle` tracks, with the themed
/// scrollbar over its right edge.
///
/// The region takes its content's height and shrinks from there, so the
/// caller only caps it: a `max_h` on the parent, or `min_h_0` inside a
/// column of fixed height. Heights stay content-based on purpose: a
/// percentage height inside an auto-sized parent, such as the modal body,
/// resolves to nothing and collapses the region.
pub(crate) fn scroll_region(
    id: &'static str,
    handle: &ScrollHandle,
    content: impl IntoElement,
) -> Div {
    div()
        .relative()
        .flex()
        .flex_col()
        .child(
            div()
                .id(id)
                .min_h_0()
                .flex()
                .flex_col()
                .overflow_y_scroll()
                .track_scroll(handle)
                .child(div().flex_none().child(content)),
        )
        .child(
            div()
                .absolute()
                .inset_0()
                .child(Scrollbar::vertical(handle)),
        )
}

/// A checkbox row with a label and optional helper text below it. The text
/// wraps within the row.
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
        .gap(ROW_GAP)
        .py(PREFERENCE_PADDING_Y)
        .child(
            dbflux_components::controls::Checkbox::new(id)
                .checked(checked)
                .aria_label(label.clone())
                .on_click(move |value: &bool, window, cx| on_toggle(*value, window, cx)),
        )
        .child(
            div()
                .flex_1()
                .min_w_0()
                .flex()
                .flex_col()
                .gap(HINT_GAP)
                .child(Text::body(label))
                .when_some(hint, |column, hint| column.child(Text::caption(hint))),
        )
}

#[cfg(test)]
pub(crate) mod test_support {
    use crate::app::AppStateEntity;
    use dbflux_storage::bootstrap::StorageRuntime;
    use gpui::{
        AppContext as _, Entity, Modifiers, Point, ScrollDelta, ScrollHandle, ScrollWheelEvent,
        TestAppContext, VisualTestContext, point, px,
    };

    pub(crate) fn test_app_state(cx: &mut TestAppContext) -> Entity<AppStateEntity> {
        cx.update(dbflux_components::theme::init);
        cx.update(dbflux_ui_base::keymap::init_keymap);

        cx.update(|cx| {
            cx.new(|_| {
                AppStateEntity::new_with_storage_runtime(
                    StorageRuntime::in_memory().expect("test storage runtime"),
                )
                .expect("test app state")
            })
        })
    }

    /// Asserts that the region `handle` tracks overflows, then scrolls with
    /// the mouse wheel over it and with the navigation keys.
    pub(crate) fn assert_scrolls_by_wheel_and_keys(
        handle: &ScrollHandle,
        window: &mut VisualTestContext,
    ) {
        let bounds = handle.bounds();
        let max = handle.max_offset().y;
        assert!(max > px(0.0), "the list does not overflow: {bounds:?}");
        assert_eq!(handle.offset().y, px(0.0));

        window.simulate_event(ScrollWheelEvent {
            position: bounds.center(),
            delta: ScrollDelta::Pixels(point(px(0.0), px(-60.0))),
            modifiers: Modifiers::default(),
            ..Default::default()
        });
        window.run_until_parked();
        assert!(
            handle.offset().y < px(0.0),
            "the wheel did not scroll the list"
        );

        window.simulate_keystrokes("home");
        window.run_until_parked();
        assert_eq!(handle.offset(), Point::default());

        window.simulate_keystrokes("down");
        window.run_until_parked();
        let after_line = handle.offset().y;
        assert!(
            after_line < px(0.0),
            "the down arrow did not scroll the list"
        );

        window.simulate_keystrokes("pagedown");
        window.run_until_parked();
        assert!(handle.offset().y < after_line || handle.offset().y == -max);

        window.simulate_keystrokes("end");
        window.run_until_parked();
        assert_eq!(handle.offset().y, -max);

        window.simulate_keystrokes("pageup");
        window.run_until_parked();
        assert!(handle.offset().y > -max);
    }
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
