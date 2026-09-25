use dbflux_app::updates::{self, ChangelogRelease, changelog};
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
use gpui_component::scroll::ScrollableElement;

use super::{
    UpdatePreference, labeled_heading, preference_row, release_header, render_section,
    set_update_preference,
};

const DIALOG_WIDTH: Pixels = px(820.0);
const UPDATES_COLUMN_WIDTH: Pixels = px(340.0);
const MARK_SIZE: Pixels = px(56.0);
const TITLE_SIZE: Pixels = px(24.0);
const HEADER_PADDING_X: Pixels = px(28.0);
const HEADER_PADDING_TOP: Pixels = px(26.0);
const HEADER_PADDING_BOTTOM: Pixels = px(22.0);
const COLUMN_PADDING_X: Pixels = Spacing::XL;
const COLUMN_PADDING_Y: Pixels = px(20.0);
const FOOTER_PADDING_Y: Pixels = px(14.0);
const BADGE_HEIGHT: Pixels = px(20.0);
const BADGE_PADDING_X: Pixels = px(7.0);
const FULL_CHANGELOG_TEXT: Pixels = px(12.5);
const COLUMN_MAX_HEIGHT: Pixels = px(440.0);

/// Highlights listed per changelog section; the full changelog is one link
/// away.
const HIGHLIGHTS_PER_SECTION: usize = 4;

/// First-run welcome: the running release's highlights and the two update
/// preferences. Enter or "Get started" closes it; Settings › Updates reopens
/// it.
pub struct WelcomeDialog {
    app_state: Entity<AppStateEntity>,
    visible: bool,
    release: Option<&'static ChangelogRelease>,
    focus: ModalFocus,
}

impl WelcomeDialog {
    pub fn new(app_state: Entity<AppStateEntity>, cx: &mut Context<Self>) -> Self {
        Self {
            app_state,
            visible: false,
            release: None,
            focus: ModalFocus::new(cx),
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(&mut self, cx: &mut Context<Self>) {
        self.release = updates::parse_version(updates::current_version())
            .and_then(|current| changelog::release_for_version(changelog::bundled(), &current));
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
            .gap(Spacing::LG)
            .px(HEADER_PADDING_X)
            .pt(HEADER_PADDING_TOP)
            .pb(HEADER_PADDING_BOTTOM)
            .border_b_1()
            .border_color(theme.border)
            .child(img(mark_path).size(MARK_SIZE).flex_shrink_0())
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XXS)
                    .min_w_0()
                    .child(
                        div()
                            .font_family(AppFonts::DISPLAY)
                            .font_weight(FontWeight::BLACK)
                            .text_size(TITLE_SIZE)
                            .text_color(theme.accent_foreground)
                            .child(dbflux_i18n::t!("updates.welcome.title").to_uppercase()),
                    )
                    .child(
                        Text::body(dbflux_i18n::t!("updates.welcome.tagline"))
                            .text_color(theme.muted_foreground),
                    ),
            )
            .child(div().flex_1())
            .child(
                div()
                    .relative()
                    .flex_shrink_0()
                    .flex()
                    .items_center()
                    .h(BADGE_HEIGHT)
                    .px(BADGE_PADDING_X)
                    .child(
                        Chamfer::new(ChamferCut::KEYCAP)
                            .fill(ChromeColors::tint(theme).opacity(0.12)),
                    )
                    .child(
                        div()
                            .font_family(AppFonts::MONO)
                            .text_size(FontSizes::LABEL)
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(ChromeColors::tint(theme))
                            .whitespace_nowrap()
                            .child(badge_text),
                    ),
            )
    }

    fn render_whats_new_column(&self, cx: &App) -> impl IntoElement {
        let theme = cx.theme();
        let tint = ChromeColors::tint(theme);

        let mut column = div()
            .id("welcome-whats-new")
            .flex_1()
            .min_w_0()
            .max_h(COLUMN_MAX_HEIGHT)
            .overflow_y_scrollbar()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .px(COLUMN_PADDING_X)
            .py(COLUMN_PADDING_Y)
            .border_r_1()
            .border_color(theme.border)
            .child(
                labeled_heading(
                    AppIcon::History,
                    tint,
                    dbflux_i18n::t!("updates.welcome.whats_new"),
                )
                .pb(Spacing::XXS),
            );

        if let Some(release) = self.release {
            column = column.child(release_header(release, cx)).children(
                release
                    .sections
                    .iter()
                    .map(|section| render_section(section, HIGHLIGHTS_PER_SECTION, cx)),
            );
        }

        column.child(
            div()
                .id("welcome-full-changelog")
                .flex()
                .items_center()
                .gap(Spacing::XXS)
                .pt(Spacing::MD)
                .cursor_pointer()
                .text_size(FULL_CHANGELOG_TEXT)
                .text_color(tint)
                .hover(|link| link.underline())
                .on_click(|_, _, cx| cx.open_url(updates::FULL_CHANGELOG_URL))
                .child(
                    Icon::new(AppIcon::ExternalLink)
                        .size(FontSizes::SM)
                        .color(tint),
                )
                .child(dbflux_i18n::t!("updates.welcome.full_changelog")),
        )
    }

    fn render_updates_column(&self, cx: &App) -> impl IntoElement {
        let theme = cx.theme();
        let settings = self.app_state.read(cx).update_settings().clone();
        let check_state = self.app_state.clone();
        let whats_new_state = self.app_state.clone();

        div()
            .w(UPDATES_COLUMN_WIDTH)
            .flex_shrink_0()
            .flex()
            .flex_col()
            .gap(Spacing::XXS)
            .px(COLUMN_PADDING_X)
            .py(COLUMN_PADDING_Y)
            .child(labeled_heading(
                AppIcon::Download,
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
            .gap(Spacing::MD)
            .px(COLUMN_PADDING_X)
            .py(FOOTER_PADDING_Y)
            .border_t_1()
            .border_color(theme.border)
            .child(
                Icon::new(AppIcon::Settings)
                    .size(FontSizes::LG)
                    .color(theme.muted_foreground),
            )
            .child(
                Text::caption(dbflux_i18n::t!("updates.welcome.change_later"))
                    .font_size(FULL_CHANGELOG_TEXT),
            )
            .child(div().flex_1())
            .child(
                Button::new(
                    "welcome-get-started",
                    dbflux_i18n::t!("updates.welcome.get_started"),
                )
                .small()
                .primary()
                .icon(AppIcon::ChevronRight)
                .kbd("Enter")
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

        // The header and footer sit on the card fill (the app background);
        // the columns get the panel color, inset by the border width so the
        // chamfer border stays visible on both sides.
        let columns = div()
            .flex()
            .min_h_0()
            .mx(px(1.0))
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
            .child(card)
            .focus_handle(self.focus.handle())
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
