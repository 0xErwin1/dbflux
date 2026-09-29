use super::layout;
use super::{SettingsSection, SettingsSectionId};
use dbflux_app::keymap::Modifiers;
use dbflux_components::primitives::Text;
use dbflux_ui_base::keymap::key_chord_from_gpui;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;

const REPOSITORY: &str = env!("CARGO_PKG_REPOSITORY");

/// The links of the page, which the keyboard cursor moves between.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AboutLink {
    ReportBug,
    ViewSource,
}

impl AboutLink {
    fn url(self) -> String {
        match self {
            AboutLink::ReportBug => format!("{REPOSITORY}/issues"),
            AboutLink::ViewSource => REPOSITORY.to_string(),
        }
    }
}

pub(super) struct AboutSection {
    content_focused: bool,
    pub(super) link_cursor: AboutLink,
}

impl AboutSection {
    pub(super) fn new(_cx: &mut Context<Self>) -> Self {
        Self {
            content_focused: false,
            link_cursor: AboutLink::ReportBug,
        }
    }

    fn link_focused(&self, link: AboutLink) -> bool {
        self.content_focused && self.link_cursor == link
    }
}

impl SettingsSection for AboutSection {
    fn section_id(&self) -> SettingsSectionId {
        SettingsSectionId::About
    }

    /// Down, Right and Tab move to the next link, Up, Left and Shift+Tab to
    /// the previous one, and Enter or Space opens the link under the cursor.
    fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.content_focused {
            return;
        }

        let chord = key_chord_from_gpui(&event.keystroke);

        match (chord.key.as_str(), chord.modifiers) {
            ("down", modifiers) | ("right", modifiers) | ("tab", modifiers)
                if modifiers == Modifiers::none() =>
            {
                self.link_cursor = AboutLink::ViewSource;
                cx.notify();
            }
            ("up", modifiers) | ("left", modifiers) if modifiers == Modifiers::none() => {
                self.link_cursor = AboutLink::ReportBug;
                cx.notify();
            }
            ("tab", modifiers) if modifiers == Modifiers::shift() => {
                self.link_cursor = AboutLink::ReportBug;
                cx.notify();
            }
            ("enter", modifiers) | ("space", modifiers) if modifiers == Modifiers::none() => {
                cx.open_url(&self.link_cursor.url());
            }
            _ => {}
        }
    }

    fn focus_in(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = true;
        cx.notify();
    }

    fn focus_out(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = false;
        cx.notify();
    }
}

impl Render for AboutSection {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        const VERSION: &str = env!("CARGO_PKG_VERSION");
        const AUTHORS: &str = env!("CARGO_PKG_AUTHORS");
        const LICENSE: &str = env!("CARGO_PKG_LICENSE");

        #[cfg(debug_assertions)]
        const PROFILE: &str = "debug";
        #[cfg(not(debug_assertions))]
        const PROFILE: &str = "release";

        // Rendered through `img` (full color) rather than the monochrome icon
        // path so the channel-specific mark — including nightly — shows in color.
        let mark_path = match dbflux_core::ReleaseChannel::current() {
            dbflux_core::ReleaseChannel::Nightly => "branding/nightly/mark-256.png",
            _ => "branding/stable/mark-256.png",
        };

        let report_bug_focused = self.link_focused(AboutLink::ReportBug);
        let view_source_focused = self.link_focused(AboutLink::ViewSource);
        let author_name = AUTHORS.split('<').next().unwrap_or(AUTHORS).trim();
        let license_display = LICENSE.replace(" OR ", " and ");
        let copyright_line = crate::labels::about_copyright(author_name);
        let license_line = crate::labels::about_license(&license_display);

        div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(dbflux_components::composites::page_header(
                dbflux_i18n::t!("settings.about.title"),
                dbflux_i18n::t!("settings.about.subtitle"),
                cx,
            ))
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scrollbar()
                    .px(crate::tokens::SettingsMetrics::BODY_PADDING_X)
                    .py(dbflux_components::tokens::Spacing::LG)
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_3()
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap_3()
                                    .child(img(mark_path).size(px(65.0)))
                                    .child(
                                        div()
                                            .flex()
                                            .flex_col()
                                            .gap_1()
                                            .child(Text::heading("DBFlux"))
                                            .child(
                                                Text::code(format!("{} ({})", VERSION, PROFILE))
                                                    .muted_foreground(),
                                            ),
                                    ),
                            )
                            .child(
                                div().child(
                                    // items_baseline + Body-wrapped fillers keeps
                                    // the link rows and the surrounding plain text
                                    // on the same baseline; bare &str children
                                    // sit on a different metric and look pulled up.
                                    div()
                                        .flex()
                                        .items_baseline()
                                        .gap_1()
                                        .child(layout::cursor_ring(
                                            report_bug_focused,
                                            div()
                                                .id("about-link-issues")
                                                .cursor_pointer()
                                                .hover(|d| d.underline())
                                                .on_click(|_, _, cx| {
                                                    cx.open_url(&AboutLink::ReportBug.url());
                                                })
                                                .child(
                                                    Text::body(dbflux_i18n::t!(
                                                        "settings.about.report_bug"
                                                    ))
                                                    .color(theme.link),
                                                ),
                                            cx,
                                        ))
                                        .child(Text::body(dbflux_i18n::t!("settings.about.or")))
                                        .child(layout::cursor_ring(
                                            view_source_focused,
                                            div()
                                                .id("about-link-repo")
                                                .cursor_pointer()
                                                .hover(|d| d.underline())
                                                .on_click(|_, _, cx| {
                                                    cx.open_url(&AboutLink::ViewSource.url());
                                                })
                                                .child(
                                                    Text::body(dbflux_i18n::t!(
                                                        "settings.about.view_source"
                                                    ))
                                                    .color(theme.link),
                                                ),
                                            cx,
                                        ))
                                        .child(Text::body(dbflux_i18n::t!(
                                            "settings.about.on_github"
                                        ))),
                                ),
                            )
                            .child(Text::body(copyright_line))
                            .child(Text::body(license_line))
                            .child(
                                div()
                                    .mt_4()
                                    .pt_4()
                                    .border_t_1()
                                    .border_color(theme.border)
                                    .flex()
                                    .flex_col()
                                    .gap_2()
                                    .child(Text::body(dbflux_i18n::t!(
                                        "settings.about.third_party_licenses"
                                    )))
                                    .child(
                                        Text::body(dbflux_i18n::t!("settings.about.lucide"))
                                            .color(theme.muted_foreground),
                                    )
                                    .child(
                                        Text::body(dbflux_i18n::t!("settings.about.simple_icons"))
                                            .color(theme.muted_foreground),
                                    ),
                            ),
                    ),
            )
    }
}

#[cfg(test)]
mod tests {
    use crate::labels::{about_copyright, about_license};

    const ABOUT_CATALOG_KEYS: &[&str] = &[
        "settings.about.title",
        "settings.about.subtitle",
        "settings.about.report_bug",
        "settings.about.or",
        "settings.about.view_source",
        "settings.about.on_github",
        "settings.about.copyright",
        "settings.about.license",
        "settings.about.third_party_licenses",
        "settings.about.lucide",
        "settings.about.simple_icons",
    ];

    #[test]
    fn settings_about_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in ABOUT_CATALOG_KEYS {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(
                    !value.is_empty(),
                    "key {key} resolved empty for locale {locale}"
                );
                assert_ne!(value, *key, "key {key} did not resolve for locale {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "key {key} fell back to the raw locale-qualified form for locale {locale}"
                );
            }
        }
    }

    #[test]
    fn settings_about_title_differs_between_locales() {
        let english = dbflux_i18n::t!("settings.about.title", locale = "en");
        let spanish = dbflux_i18n::t!("settings.about.title", locale = "es");

        assert_eq!(english, "About");
        assert_eq!(spanish, "Acerca de");
        assert_ne!(english, spanish);
    }

    #[test]
    fn about_copyright_embeds_author_name() {
        let en = about_copyright("Jane Doe");
        let es = about_copyright("Jane Doe");

        assert!(en.contains("Jane Doe"));
        assert!(es.contains("Jane Doe"));
    }

    #[test]
    fn about_license_embeds_license_identifier() {
        let en = about_license("MIT and Apache-2.0");

        assert!(en.contains("MIT and Apache-2.0"));
    }
}
