use crate::keymap::{Command, ContextId};
use dbflux_app::updates::{self, ChangelogRelease, SectionKind, changelog};
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::{Modal, ModalFocus};
use dbflux_components::primitives::{Chamfer, Icon, Text};
use dbflux_components::tokens::{ChamferCut, ChromeColors, FontSizes, Spacing};
use dbflux_components::typography::AppFonts;
use dbflux_core::{LogErr, ReleaseChannel};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::updates::channel_label;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

use super::{
    LINK_TEXT_SIZE, UpdatePreference, full_changelog_link, labeled_heading, preference_row,
    release_header, render_section, scroll_region, set_update_preference,
};

const DIALOG_WIDTH: Pixels = px(1000.0);
const WHATS_NEW_COLUMN_WIDTH: Pixels = px(330.0);
const MARK_SIZE: Pixels = px(56.0);
const TITLE_SIZE: Pixels = px(24.0);
const HEADER_GAP: Pixels = Spacing::LG;
const HEADER_PADDING_X: Pixels = px(28.0);
const HEADER_PADDING_TOP: Pixels = px(26.0);
const HEADER_PADDING_BOTTOM: Pixels = px(22.0);
const COLUMN_PADDING_X: Pixels = Spacing::XL;
const COLUMN_PADDING_Y: Pixels = px(20.0);
const WHATS_NEW_COLUMN_GAP: Pixels = Spacing::XS;
const UPDATES_COLUMN_GAP: Pixels = Spacing::XXS;
const FULL_CHANGELOG_PADDING_TOP: Pixels = px(10.0);
const FOOTER_GAP: Pixels = px(10.0);
const FOOTER_PADDING_Y: Pixels = px(14.0);
const FOOTER_ICON_SIZE: Pixels = px(14.0);
const BADGE_HEIGHT: Pixels = px(20.0);
const BADGE_PADDING_X: Pixels = px(7.0);

/// Tint share of the version badge fill.
const BADGE_FILL_OPACITY: f32 = 0.12;

/// Element id of the scrolling release highlights.
const HIGHLIGHTS_ID: &str = "welcome-whats-new-list";

/// Highlights listed per changelog section, as the board shows them: four
/// additions, two of anything else. The full changelog is one link away.
fn highlights_per_section(kind: &SectionKind) -> usize {
    match kind {
        SectionKind::Added => 4,
        _ => 2,
    }
}

/// First-run welcome: the running release's highlights and the two update
/// preferences. Enter or "Get started" closes it; Settings › Updates reopens
/// it.
pub struct WelcomeDialog {
    app_state: Entity<AppStateEntity>,
    visible: bool,
    release: Option<&'static ChangelogRelease>,
    focus: ModalFocus,
    highlights_scroll: ScrollHandle,
}

impl WelcomeDialog {
    pub fn new(app_state: Entity<AppStateEntity>, cx: &mut Context<Self>) -> Self {
        Self {
            app_state,
            visible: false,
            release: None,
            focus: ModalFocus::new(cx),
            highlights_scroll: ScrollHandle::new(),
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(&mut self, cx: &mut Context<Self>) {
        self.release = updates::parse_version(updates::current_version())
            .and_then(|current| changelog::release_for_version(changelog::bundled(), &current));
        self.highlights_scroll.set_offset(Point::default());
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

    fn render_header(&self, cx: &App) -> impl IntoElement {
        let theme = cx.theme();
        let tint = ChromeColors::tint(theme);
        let mark_path = match ReleaseChannel::current() {
            ReleaseChannel::Nightly => "branding/nightly/mark-256.png",
            ReleaseChannel::Stable | ReleaseChannel::Rc => "branding/stable/mark-256.png",
        };
        let badge_text = format!(
            "{} \u{b7} {}",
            updates::display_version(updates::current_version()),
            channel_label(ReleaseChannel::current())
        );

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(HEADER_GAP)
            .px(HEADER_PADDING_X)
            .pt(HEADER_PADDING_TOP)
            .pb(HEADER_PADDING_BOTTOM)
            .border_b_1()
            .border_color(theme.border)
            .child(img(mark_path).size(MARK_SIZE).flex_shrink_0())
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XXS)
                    .child(
                        div()
                            .font_family(AppFonts::DISPLAY)
                            .font_weight(FontWeight::BLACK)
                            .text_size(TITLE_SIZE)
                            .text_color(ChromeColors::strong(theme))
                            .child(dbflux_i18n::t!("updates.welcome.title").to_uppercase()),
                    )
                    .child(
                        Text::body(dbflux_i18n::t!("updates.welcome.tagline"))
                            .text_color(theme.muted_foreground),
                    ),
            )
            .child(
                div()
                    .relative()
                    .flex_shrink_0()
                    .flex()
                    .items_center()
                    .h(BADGE_HEIGHT)
                    .px(BADGE_PADDING_X)
                    .child(Chamfer::new(ChamferCut::KEYCAP).fill(tint.opacity(BADGE_FILL_OPACITY)))
                    .child(
                        div()
                            .font_family(AppFonts::MONO)
                            .text_size(FontSizes::LABEL)
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(tint)
                            .whitespace_nowrap()
                            .child(badge_text),
                    ),
            )
    }

    fn render_whats_new_column(&self, cx: &App) -> impl IntoElement {
        let theme = cx.theme();

        let mut highlights = div().flex().flex_col().gap(WHATS_NEW_COLUMN_GAP);

        if let Some(release) = self.release {
            highlights = highlights.child(release_header(release, cx)).children(
                release.sections.iter().map(|section| {
                    render_section(
                        section,
                        highlights_per_section(&section.kind),
                        Spacing::XS,
                        cx,
                    )
                }),
            );
        }

        div()
            .w(WHATS_NEW_COLUMN_WIDTH)
            .flex_shrink_0()
            .min_h_0()
            .flex()
            .flex_col()
            .gap(WHATS_NEW_COLUMN_GAP)
            .px(COLUMN_PADDING_X)
            .py(COLUMN_PADDING_Y)
            .border_r_1()
            .border_color(theme.border)
            .child(
                labeled_heading(
                    AppIcon::History,
                    ChromeColors::tint(theme),
                    dbflux_i18n::t!("updates.welcome.whats_new"),
                )
                .flex_shrink_0()
                .pb(Spacing::XXS),
            )
            .child(
                scroll_region(HIGHLIGHTS_ID, &self.highlights_scroll, highlights)
                    .flex_grow(1.0)
                    .min_h_0(),
            )
            .child(
                full_changelog_link("welcome-full-changelog", cx)
                    .flex_shrink_0()
                    .pt(FULL_CHANGELOG_PADDING_TOP),
            )
    }

    fn render_updates_column(&self, cx: &App) -> impl IntoElement {
        let theme = cx.theme();
        let settings = self.app_state.read(cx).update_settings().clone();
        let check_state = self.app_state.clone();
        let whats_new_state = self.app_state.clone();

        div()
            .flex_1()
            .min_w_0()
            .flex()
            .flex_col()
            .gap(UPDATES_COLUMN_GAP)
            .px(COLUMN_PADDING_X)
            .py(COLUMN_PADDING_Y)
            .child(labeled_heading(
                AppIcon::Bell,
                ChromeColors::tint(theme),
                dbflux_i18n::t!("updates.welcome.updates"),
            ))
            .child(preference_row(
                "welcome-check-for-updates",
                dbflux_i18n::t!("updates.welcome.notify"),
                Some(dbflux_i18n::t!("updates.welcome.notify_hint")),
                settings.check_for_updates_on_startup,
                move |value, _, cx| {
                    set_update_preference(&check_state, UpdatePreference::CheckOnStartup, value, cx)
                },
            ))
            .child(preference_row(
                "welcome-show-whats-new",
                dbflux_i18n::t!("updates.welcome.show_whats_new"),
                Some(dbflux_i18n::t!("updates.welcome.show_whats_new_hint")),
                settings.show_whats_new_after_update,
                move |value, _, cx| {
                    set_update_preference(
                        &whats_new_state,
                        UpdatePreference::ShowWhatsNew,
                        value,
                        cx,
                    )
                },
            ))
    }

    fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(FOOTER_GAP)
            .px(COLUMN_PADDING_X)
            .py(FOOTER_PADDING_Y)
            .border_t_1()
            .border_color(theme.border)
            .child(
                Icon::new(AppIcon::Settings)
                    .size(FOOTER_ICON_SIZE)
                    .color(theme.muted_foreground),
            )
            .child(
                div().flex_1().min_w_0().child(
                    Text::caption(dbflux_i18n::t!("updates.welcome.change_later"))
                        .font_size(LINK_TEXT_SIZE),
                ),
            )
            .child(
                Button::new(
                    "welcome-get-started",
                    dbflux_i18n::t!("updates.welcome.get_started"),
                )
                .primary()
                .icon(AppIcon::ChevronRight)
                .when_some(
                    dbflux_ui_base::keymap::shortcut_label(ContextId::Modal, Command::Execute),
                    Button::kbd,
                )
                .on_click(cx.listener(|this, _, _, cx| this.close(cx))),
            )
    }
}

impl Render for WelcomeDialog {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        self.focus.apply_pending(window, cx);

        let theme = cx.theme();

        // The board draws the header and footer on the app background and
        // only the two columns on the panel fill, with no outline around the
        // card: its ring is clipped away by the cut.
        let columns = div()
            .flex()
            .min_h_0()
            .bg(theme.popover)
            .child(self.render_whats_new_column(cx))
            .child(self.render_updates_column(cx));

        let card = div()
            .flex()
            .flex_col()
            .min_h_0()
            .child(self.render_header(cx))
            .child(columns)
            .child(self.render_footer(cx));

        let card_fill = cx.theme().background;
        let dialog = cx.entity().downgrade();
        let dialog_for_enter = dialog.clone();

        Modal::new(dbflux_i18n::t!("updates.welcome.title"))
            .id("welcome-dialog")
            .without_header()
            .width(DIALOG_WIDTH)
            .cut(ChamferCut::CARD)
            .fill(card_fill)
            .border(gpui::transparent_black())
            .child(card)
            .focus_handle(self.focus.handle())
            .scroll_with_keys(&self.highlights_scroll)
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
    use super::super::test_support::{assert_scrolls_by_wheel_and_keys, test_app_state};
    use super::{SectionKind, WelcomeDialog, highlights_per_section};
    use gpui::{TestAppContext, px, size};

    #[gpui::test]
    fn the_highlights_scroll_when_the_window_is_short(cx: &mut TestAppContext) {
        let app_state = test_app_state(cx);
        let (dialog, window) = cx.add_window_view(move |_, cx| WelcomeDialog::new(app_state, cx));
        window.simulate_resize(size(px(1200.0), px(460.0)));

        dialog.update(window, |dialog, cx| dialog.open(cx));
        window.run_until_parked();

        let handle = dialog.read_with(window, |dialog, _| dialog.highlights_scroll.clone());
        assert_scrolls_by_wheel_and_keys(&handle, window);
    }

    #[test]
    fn highlights_follow_the_board_counts() {
        assert_eq!(highlights_per_section(&SectionKind::Added), 4);
        assert_eq!(highlights_per_section(&SectionKind::Fixed), 2);
        assert_eq!(highlights_per_section(&SectionKind::Changed), 2);
    }

    #[test]
    fn welcome_copy_resolves_in_every_locale() {
        for key in [
            "updates.welcome.title",
            "updates.welcome.tagline",
            "updates.welcome.whats_new",
            "updates.welcome.updates",
            "updates.welcome.notify",
            "updates.welcome.notify_hint",
            "updates.welcome.show_whats_new",
            "updates.welcome.show_whats_new_hint",
            "updates.welcome.change_later",
            "updates.welcome.get_started",
            "updates.welcome.full_changelog",
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
    }
}
