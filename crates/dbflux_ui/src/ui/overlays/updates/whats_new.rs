use crate::keymap::{Command, ContextId};
use dbflux_app::updates::{self, ChangelogRelease, changelog};
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::{Modal, ModalFocus};
use dbflux_components::primitives::{Chamfer, Icon, Text};
use dbflux_components::tokens::{ChamferCut, ChromeColors, ModalMetrics, Spacing};
use dbflux_core::LogErr;
use dbflux_ui_base::AppStateEntity;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

use super::{
    UpdatePreference, preference_row, release_header, render_section, scroll_region,
    set_update_preference,
};

const DIALOG_WIDTH: Pixels = px(560.0);
const CHANGELOG_MAX_HEIGHT: Pixels = px(420.0);
const CHANGELOG_PADDING_X: Pixels = Spacing::LG;
const CHANGELOG_PADDING_Y: Pixels = px(14.0);
const CHANGELOG_GAP: Pixels = px(2.0);
const RELEASE_DIVIDER_TOP: Pixels = Spacing::MD;
const RELEASE_DIVIDER_BOTTOM: Pixels = Spacing::SM;
const RANGE_GAP: Pixels = px(10.0);
const RANGE_ICON_SIZE: Pixels = px(13.0);

/// Element id of the scrolling changelog list.
const CHANGELOG_ID: &str = "whats-new-changelog";

/// Every entry of a release is listed; the list scrolls instead.
const ENTRIES_PER_SECTION: usize = usize::MAX;

/// What the dialog lists.
#[derive(Debug, Clone, PartialEq, Eq)]
enum WhatsNewContent {
    /// First start after an update: every release after `since`.
    SinceLastRun { since: String },
    /// Opened by hand: the release the user is running.
    RunningRelease,
}

/// "What is new": the changelog of every release since the last run, shown
/// once on the first start of a newer version, or the running release when
/// opened from Settings.
pub struct WhatsNewDialog {
    app_state: Entity<AppStateEntity>,
    visible: bool,
    content: WhatsNewContent,
    releases: Vec<&'static ChangelogRelease>,
    focus: ModalFocus,
    changelog_scroll: ScrollHandle,
}

impl WhatsNewDialog {
    pub fn new(app_state: Entity<AppStateEntity>, cx: &mut Context<Self>) -> Self {
        Self {
            app_state,
            visible: false,
            content: WhatsNewContent::RunningRelease,
            releases: Vec::new(),
            focus: ModalFocus::new(cx),
            changelog_scroll: ScrollHandle::new(),
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    /// Opens the dialog for the releases after `since`, or for the running
    /// release when `since` is `None` or names no parseable version.
    pub fn open(&mut self, since: Option<String>, cx: &mut Context<Self>) {
        self.open_for_version(since, updates::current_version(), cx);
    }

    /// Opens the dialog as if `current_version` were the running version.
    fn open_for_version(
        &mut self,
        since: Option<String>,
        current_version: &str,
        cx: &mut Context<Self>,
    ) {
        let (content, releases) = releases_to_show(since, current_version);

        self.content = content;
        self.releases = releases;
        self.changelog_scroll.set_offset(Point::default());
        self.visible = true;
        self.focus.focus_on_next_render();
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        if !self.visible {
            return;
        }

        self.visible = false;
        self.focus.restore(cx);
        cx.notify();
    }

    fn render_range(&self, cx: &App) -> impl IntoElement {
        let theme = cx.theme();
        let muted = theme.muted_foreground;
        let current = updates::display_version(updates::current_version());

        let (since, summary) = match &self.content {
            WhatsNewContent::SinceLastRun { since } => (
                Some(since.clone()),
                releases_since_label(self.releases.len()),
            ),
            WhatsNewContent::RunningRelease => {
                (None, dbflux_i18n::t!("updates.whats_new.this_release"))
            }
        };

        div()
            .flex()
            .flex_wrap()
            .items_center()
            .gap(RANGE_GAP)
            .when_some(since, |row, since| {
                row.child(Text::code(updates::display_version(&since)).text_color(muted))
                    .child(
                        Icon::new(AppIcon::ChevronRight)
                            .size(RANGE_ICON_SIZE)
                            .color(muted),
                    )
            })
            .child(
                Text::code(current)
                    .font_weight(FontWeight::BOLD)
                    .text_color(ChromeColors::strong(theme)),
            )
            .child(Text::caption(summary))
    }

    fn render_changelog(&self, cx: &App) -> impl IntoElement {
        let theme = cx.theme();

        let mut list = div()
            .flex()
            .flex_col()
            .gap(CHANGELOG_GAP)
            .px(CHANGELOG_PADDING_X)
            .py(CHANGELOG_PADDING_Y);

        if self.releases.is_empty() {
            list = list.child(Text::caption(dbflux_i18n::t!("updates.whats_new.empty")));
        }

        for (index, release) in self.releases.iter().enumerate() {
            if index > 0 {
                list = list.child(
                    div()
                        .h(px(1.0))
                        .mt(RELEASE_DIVIDER_TOP)
                        .mb(RELEASE_DIVIDER_BOTTOM)
                        .bg(theme.border),
                );
            }

            list = list.child(release_header(release, cx)).children(
                release
                    .sections
                    .iter()
                    .map(|section| render_section(section, ENTRIES_PER_SECTION, Spacing::XXS, cx)),
            );
        }

        div()
            .relative()
            .flex()
            .flex_col()
            .max_h(CHANGELOG_MAX_HEIGHT)
            .child(
                Chamfer::new(ChamferCut::INPUT)
                    .fill(theme.background)
                    .border(theme.border),
            )
            .child(scroll_region(CHANGELOG_ID, &self.changelog_scroll, list).min_h_0())
    }

    fn render_footer(&self, cx: &mut Context<Self>) -> AnyElement {
        div()
            .flex()
            .items_center()
            .gap(ModalMetrics::FOOTER_GAP)
            .child(
                Button::new(
                    "whats-new-full-changelog",
                    dbflux_i18n::t!("updates.whats_new.full_changelog"),
                )
                .secondary()
                .icon(AppIcon::ExternalLink)
                .on_click(|_, _, cx| cx.open_url(updates::FULL_CHANGELOG_URL)),
            )
            .child(
                Button::new(
                    "whats-new-close-button",
                    dbflux_i18n::t!("updates.whats_new.close"),
                )
                .primary()
                .when_some(
                    dbflux_ui_base::keymap::shortcut_label(ContextId::Modal, Command::Cancel),
                    Button::kbd,
                )
                .on_click(cx.listener(|this, _, _, cx| this.close(cx))),
            )
            .into_any_element()
    }
}

/// "N releases since you last opened DBFlux", with the singular form for one.
fn releases_since_label(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!("updates.whats_new.releases_since.one")
    } else {
        dbflux_i18n::t!("updates.whats_new.releases_since.many", count = count)
    }
}

/// Decides what the dialog lists for a request.
fn releases_to_show(
    since: Option<String>,
    current_version: &str,
) -> (WhatsNewContent, Vec<&'static ChangelogRelease>) {
    let releases = changelog::bundled();
    let current = updates::parse_version(current_version);

    let since_version = since
        .as_deref()
        .and_then(updates::parse_version)
        .zip(since.clone());

    match (since_version, current) {
        (Some((since_version, since_label)), Some(current)) => (
            WhatsNewContent::SinceLastRun { since: since_label },
            changelog::releases_between(releases, &since_version, &current),
        ),
        (_, Some(current)) => (
            WhatsNewContent::RunningRelease,
            changelog::release_for_version(releases, &current)
                .into_iter()
                .collect(),
        ),
        (_, None) => (WhatsNewContent::RunningRelease, Vec::new()),
    }
}

impl Render for WhatsNewDialog {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        self.focus.apply_pending(window, cx);

        let show_after_update = self
            .app_state
            .read(cx)
            .update_settings()
            .show_whats_new_after_update;
        let app_state = self.app_state.clone();

        let body = div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(self.render_range(cx))
            .child(self.render_changelog(cx))
            .child(preference_row(
                "whats-new-show-after-update",
                dbflux_i18n::t!("updates.whats_new.show_after_update"),
                None,
                show_after_update,
                move |value, _, cx| {
                    set_update_preference(&app_state, UpdatePreference::ShowWhatsNew, value, cx)
                },
            ));

        let footer = self.render_footer(cx);
        let dialog = cx.entity().downgrade();
        let dialog_for_enter = dialog.clone();

        Modal::new(dbflux_i18n::t!("updates.whats_new.title"))
            .id("whats-new-dialog")
            .icon(AppIcon::History)
            .width(DIALOG_WIDTH)
            .cut(ChamferCut::CARD)
            .body(body)
            .footer(footer)
            .focus_handle(self.focus.handle())
            .scroll_with_keys(&self.changelog_scroll)
            .on_close(move |_, cx| {
                dialog.update(cx, |dialog, cx| dialog.close(cx)).log_err();
            })
            .on_confirm(move |_, cx| {
                dialog_for_enter
                    .update(cx, |dialog, cx| dialog.close(cx))
                    .log_err();
            })
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    // Explicit imports: the parent's `gpui::*` glob would make `#[test]`
    // resolve to `gpui::test`, whose expansion recurses without bound.
    use super::super::release_label;
    use super::super::test_support::{assert_scrolls_by_wheel_and_keys, test_app_state};
    use super::{
        CHANGELOG_MAX_HEIGHT, WhatsNewContent, WhatsNewDialog, releases_since_label,
        releases_to_show, updates,
    };
    use dbflux_app::updates::ReleaseHeading;
    use gpui::TestAppContext;

    #[gpui::test]
    fn the_changelog_scrolls_with_the_wheel_and_the_keyboard(cx: &mut TestAppContext) {
        let app_state = test_app_state(cx);
        let (dialog, window) = cx.add_window_view(move |_, cx| WhatsNewDialog::new(app_state, cx));

        // Pinned to a release whose section overflows, so the test does not
        // depend on the size of the running version's changelog section.
        dialog.update(window, |dialog, cx| {
            dialog.open_for_version(None, "0.8.0", cx)
        });
        window.run_until_parked();

        let handle = dialog.read_with(window, |dialog, _| dialog.changelog_scroll.clone());
        assert!(handle.bounds().size.height <= CHANGELOG_MAX_HEIGHT);

        assert_scrolls_by_wheel_and_keys(&handle, window);
    }

    #[test]
    fn a_manual_open_on_a_dev_build_lists_the_last_release() {
        let (content, releases) = releases_to_show(None, "0.8.0-dev.0");

        assert_eq!(content, WhatsNewContent::RunningRelease);
        assert_eq!(releases.len(), 1);
        assert_eq!(release_label(releases[0]), "0.7.0");
    }

    #[test]
    fn a_previous_version_lists_the_releases_after_it() {
        let (content, releases) = releases_to_show(Some("0.6.0".to_string()), "0.7.0");

        assert_eq!(
            content,
            WhatsNewContent::SinceLastRun {
                since: "0.6.0".to_string()
            }
        );
        assert_eq!(
            releases.first().map(|release| release.heading.clone()),
            updates::parse_version("0.7.0").map(ReleaseHeading::Version)
        );
        assert!(
            releases
                .iter()
                .all(|release| release_label(release) != "0.6.0")
        );
    }

    #[test]
    fn a_manual_open_lists_the_running_release() {
        let (content, releases) = releases_to_show(None, "0.7.0");

        assert_eq!(content, WhatsNewContent::RunningRelease);
        assert_eq!(releases.len(), 1);
        assert_eq!(release_label(releases[0]), "0.7.0");

        let (garbage_content, _) = releases_to_show(Some("garbage".to_string()), "0.7.0");
        assert_eq!(garbage_content, WhatsNewContent::RunningRelease);
    }

    #[test]
    fn whats_new_copy_resolves_in_every_locale() {
        for key in [
            "updates.whats_new.title",
            "updates.whats_new.releases_since.one",
            "updates.whats_new.releases_since.many",
            "updates.whats_new.this_release",
            "updates.whats_new.unreleased",
            "updates.whats_new.show_after_update",
            "updates.whats_new.full_changelog",
            "updates.whats_new.close",
            "updates.whats_new.empty",
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

        assert!(releases_since_label(2).contains('2'));
    }
}
